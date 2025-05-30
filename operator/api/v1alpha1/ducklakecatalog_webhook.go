package v1alpha1

import (
	"net/url"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var ducklakecataloglog = logf.Log.WithName("ducklakecatalog-resource")

func (r *DuckLakeCatalog) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-ducklake-featherman-dev-v1alpha1-ducklakecatalog,mutating=true,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklakecatalogs,verbs=create;update,versions=v1alpha1,name=mducklakecatalog.kb.io,admissionReviewVersions=v1

// Default implements admission.Defaulter so a webhook will be registered for the type
func (r *DuckLakeCatalog) Default() {
	ducklakecataloglog.Info("default", "name", r.Name)

	// Set default backup retention days if backup policy exists but retention is not set
	if r.Spec.BackupPolicy != nil && r.Spec.BackupPolicy.RetentionDays == 0 {
		r.Spec.BackupPolicy.RetentionDays = 7
	}
}

//+kubebuilder:webhook:path=/validate-ducklake-featherman-dev-v1alpha1-ducklakecatalog,mutating=false,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklakecatalogs,verbs=create;update,versions=v1alpha1,name=vducklakecatalog.kb.io,admissionReviewVersions=v1

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeCatalog) ValidateCreate() (admission.Warnings, error) {
	ducklakecataloglog.Info("validate create", "name", r.Name)

	var allErrs field.ErrorList

	// Validate object store endpoint URL
	if _, err := url.Parse(r.Spec.ObjectStore.Endpoint); err != nil {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec").Child("objectStore").Child("endpoint"),
			r.Spec.ObjectStore.Endpoint,
			"must be a valid URL",
		))
	}

	// Validate storage size format
	if !isValidStorageSize(r.Spec.Size) {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec").Child("size"),
			r.Spec.Size,
			"must be a valid storage size (e.g., 1Gi, 500Mi)",
		))
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeCatalog"},
		r.Name, allErrs)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeCatalog) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	ducklakecataloglog.Info("validate update", "name", r.Name)

	var allErrs field.ErrorList

	// Validate immutable fields
	oldCatalog := old.(*DuckLakeCatalog)
	if oldCatalog.Spec.StorageClass != r.Spec.StorageClass {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec").Child("storageClass"),
			"field is immutable",
		))
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeCatalog"},
		r.Name, allErrs)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeCatalog) ValidateDelete() (admission.Warnings, error) {
	ducklakecataloglog.Info("validate delete", "name", r.Name)
	return nil, nil
}

// isValidStorageSize validates storage size format
func isValidStorageSize(size string) bool {
	// Parse the storage size using k8s resource quantity
	quantity, err := resource.ParseQuantity(size)
	if err != nil {
		return false
	}

	// Ensure size is positive
	if quantity.Sign() <= 0 {
		return false
	}

	// Ensure size is at least 1Gi
	minSize := resource.MustParse("1Gi")
	if quantity.Cmp(minSize) < 0 {
		return false
	}

	return true
}
