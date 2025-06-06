package v1alpha1

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/TFMV/featherman/operator/internal/logger"
)

func (r *DuckLakeTable) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-ducklake-featherman-dev-v1alpha1-ducklaketable,mutating=true,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklaketables,verbs=create;update,versions=v1alpha1,name=mducklaketable.kb.io,admissionReviewVersions=v1

// Default implements admission.Defaulter so a webhook will be registered for the type
func (r *DuckLakeTable) Default() {
	l := logger.FromContext(context.Background()).With().
		Str("webhook", "DuckLakeTable").
		Str("operation", "default").
		Str("name", r.Name).
		Logger()

	l.Info().Msg("Applying default values")

	// Set default compression if not specified
	if r.Spec.Format.Compression == "" {
		l.Debug().Msg("Setting default compression to ZSTD")
		r.Spec.Format.Compression = CompressionZSTD
	}

	// Set default mode if not specified
	if r.Spec.Mode == "" {
		l.Debug().Msg("Setting default mode to Append")
		r.Spec.Mode = TableModeAppend
	}

	l.Info().
		Str("compression", string(r.Spec.Format.Compression)).
		Str("mode", string(r.Spec.Mode)).
		Msg("Defaults applied successfully")
}

//+kubebuilder:webhook:path=/validate-ducklake-featherman-dev-v1alpha1-ducklaketable,mutating=false,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklaketables,verbs=create;update,versions=v1alpha1,name=vducklaketable.kb.io,admissionReviewVersions=v1

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateCreate() (admission.Warnings, error) {
	l := logger.FromContext(context.Background()).With().
		Str("webhook", "DuckLakeTable").
		Str("operation", "validate_create").
		Str("name", r.Name).
		Logger()

	l.Info().Msg("Validating table creation")

	var allErrs field.ErrorList

	// Validate table name format
	if !isValidTableName(r.Spec.Name) {
		l.Error().
			Str("tableName", r.Spec.Name).
			Msg("Invalid table name format")
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec").Child("name"),
			r.Spec.Name,
			"must start with a letter and contain only letters, numbers, and underscores",
		))
	}

	// Validate columns
	if len(r.Spec.Columns) == 0 {
		l.Error().Msg("No columns defined")
		allErrs = append(allErrs, field.Required(
			field.NewPath("spec").Child("columns"),
			"at least one column must be defined",
		))
	}

	// Validate partitioning columns exist in the schema
	for _, partition := range r.Spec.Format.Partitioning {
		found := false
		for _, col := range r.Spec.Columns {
			if col.Name == partition {
				found = true
				break
			}
		}
		if !found {
			l.Error().
				Str("partition", partition).
				Msg("Partition column not found in schema")
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec").Child("format").Child("partitioning"),
				partition,
				"partition column must exist in table schema",
			))
		}
	}

	if len(allErrs) == 0 {
		l.Info().Msg("Table creation validation successful")
		return nil, nil
	}

	l.Error().
		Int("errorCount", len(allErrs)).
		Msg("Table creation validation failed")
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeTable"},
		r.Name, allErrs)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	l := logger.FromContext(context.Background()).With().
		Str("webhook", "DuckLakeTable").
		Str("operation", "validate_update").
		Str("name", r.Name).
		Logger()

	l.Info().Msg("Validating table update")

	var allErrs field.ErrorList
	oldTable := old.(*DuckLakeTable)

	// Validate immutable fields
	if oldTable.Spec.Name != r.Spec.Name {
		l.Error().
			Str("oldName", oldTable.Spec.Name).
			Str("newName", r.Spec.Name).
			Msg("Attempt to modify immutable table name")
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec").Child("name"),
			"field is immutable",
		))
	}

	// Cannot change column types
	for i, newCol := range r.Spec.Columns {
		if i < len(oldTable.Spec.Columns) {
			oldCol := oldTable.Spec.Columns[i]
			if oldCol.Name == newCol.Name && oldCol.Type != newCol.Type {
				l.Error().
					Str("column", oldCol.Name).
					Str("oldType", string(oldCol.Type)).
					Str("newType", string(newCol.Type)).
					Msg("Attempt to modify immutable column type")
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec").Child("columns").Index(i).Child("type"),
					"column type is immutable",
				))
			}
		}
	}

	if len(allErrs) == 0 {
		l.Info().Msg("Table update validation successful")
		return nil, nil
	}

	l.Error().
		Int("errorCount", len(allErrs)).
		Msg("Table update validation failed")
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeTable"},
		r.Name, allErrs)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateDelete() (admission.Warnings, error) {
	l := logger.FromContext(context.Background()).With().
		Str("webhook", "DuckLakeTable").
		Str("operation", "validate_delete").
		Str("name", r.Name).
		Logger()

	l.Info().Msg("Validating table deletion")
	return nil, nil
}

// isValidTableName validates table name format
func isValidTableName(name string) bool {
	if len(name) == 0 {
		return false
	}
	if !isLetter(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !isAlphanumericOrUnderscore(name[i]) {
			return false
		}
	}
	return true
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isAlphanumericOrUnderscore(c byte) bool {
	return isLetter(c) || (c >= '0' && c <= '9') || c == '_'
}
