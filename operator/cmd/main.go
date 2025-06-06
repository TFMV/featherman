package main

import (
	"context"
	"crypto/tls"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"time"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/controller"
	"github.com/TFMV/featherman/operator/internal/duckdb"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/sql"
	"github.com/TFMV/featherman/operator/internal/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(ducklakev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var catalogPath string
	var logLevel string
	var logFormat string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&catalogPath, "catalog-path", "/var/lib/featherman", "Path to store DuckDB catalog files")
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.StringVar(&logFormat, "log-format", "console", "Log format (json or console)")
	flag.Parse()

	// Initialize enhanced logger
	logger.Configure(logger.Config{
		ServiceName: "featherman-operator",
		Environment: os.Getenv("ENVIRONMENT"),
		LogLevel:    logLevel,
		Format:      logFormat,
	})

	// Set up controller-runtime logger
	ctrl.SetLogger(logger.NewControllerRuntimeLogger())

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	disableHTTP2 := func(c *tls.Config) {
		logger.Info().Msg("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		logger.Info().
			Str("webhook-cert-path", webhookCertPath).
			Str("webhook-cert-name", webhookCertName).
			Str("webhook-cert-key", webhookCertKey).
			Msg("Initializing webhook certificate watcher using provided certificates")

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	if len(metricsCertPath) > 0 {
		logger.Info().
			Str("metrics-cert-path", metricsCertPath).
			Str("metrics-cert-name", metricsCertName).
			Str("metrics-cert-key", metricsCertKey).
			Msg("Initializing metrics certificate watcher using provided certificates")

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			logger.Error().Err(err).Msg("to initialize metrics certificate watcher")
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "featherman-operator-lock",
	})
	if err != nil {
		logger.Error().Err(err).Msg("unable to start manager")
		os.Exit(1)
	}

	// Initialize metrics with the controller-runtime registry
	metrics.InitMetrics(ctrlmetrics.Registry.(prometheus.Registerer))

	// Create JobManager
	k8sClient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		logger.Error().Err(err).Msg("unable to create kubernetes client")
		os.Exit(1)
	}
	jobManager := duckdb.NewJobManager(k8sClient)

	// Create SQL Generator
	sqlGen := sql.NewGenerator()

	// Create S3 client for health checks
	customEndpoint := "http://minio.minio-test:9000"
	s3Client := s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"minioadmin", // Default MinIO access key
			"minioadmin", // Default MinIO secret key
			"",
		),
	}, func(o *s3.Options) {
		o.BaseEndpoint = &customEndpoint
		o.UsePathStyle = true
	})

	// Add health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error().Err(err).Msg("unable to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error().Err(err).Msg("unable to set up ready check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("s3", func(_ *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return err
	}); err != nil {
		logger.Error().Err(err).Msg("unable to set up S3 health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("kubernetes", func(_ *http.Request) error {
		return k8sClient.RESTClient().Get().AbsPath("/healthz").Do(context.Background()).Error()
	}); err != nil {
		logger.Error().Err(err).Msg("unable to set up Kubernetes health check")
		os.Exit(1)
	}

	// Initialize controllers
	catalogReconciler := &controller.DuckLakeCatalogReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("ducklakecatalog-controller"),
		Storage:  storage.NewObjectStore(s3Client),
	}

	tableReconciler := &controller.DuckLakeTableReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Recorder:    mgr.GetEventRecorderFor("ducklaketable-controller"),
		JobManager:  jobManager,
		SQLGen:      sqlGen,
		PoolManager: nil, // Will be set after pool reconciler is created
	}

	// Create logger for pool controller
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	poolReconciler := &controller.DuckLakePoolReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("ducklakepool-controller"),
		Logger:    &logger,
		K8sClient: k8sClient,
	}

	// Now set the pool manager reference
	tableReconciler.PoolManager = poolReconciler

	if err := catalogReconciler.SetupWithManager(mgr); err != nil {
		logger.Error().
			Err(err).
			Str("controller", "DuckLakeCatalog").
			Msg("unable to create controller")
		os.Exit(1)
	}

	if err := tableReconciler.SetupWithManager(mgr); err != nil {
		logger.Error().
			Err(err).
			Str("controller", "DuckLakeTable").
			Msg("unable to create controller")
		os.Exit(1)
	}

	if err := poolReconciler.SetupWithManager(mgr); err != nil {
		logger.Error().
			Err(err).
			Str("controller", "DuckLakePool").
			Msg("unable to create controller")
		os.Exit(1)
	}

	if metricsCertWatcher != nil {
		logger.Info().Msg("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			logger.Error().Err(err).Msg("unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		logger.Info().Msg("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			logger.Error().Err(err).Msg("unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	logger.Info().Msg("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error().Err(err).Msg("problem running manager")
		os.Exit(1)
	}
}
