package v1alpha1

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var ducklaketablelog = logf.Log.WithName("ducklaketable-resource")

func (r *DuckLakeTable) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/mutate-ducklake-featherman-dev-v1alpha1-ducklaketable,mutating=true,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklaketables,verbs=create;update,versions=v1alpha1,name=mducklaketable.kb.io,admissionReviewVersions=v1

// Default implements admission.Defaulter so a webhook will be registered for the type
func (r *DuckLakeTable) Default() {
	ducklaketablelog.Info("default", "name", r.Name)

	// Set default compression if not specified
	if r.Spec.Format.Compression == "" {
		r.Spec.Format.Compression = CompressionZSTD
	}

	// Set default mode if not specified
	if r.Spec.Mode == "" {
		r.Spec.Mode = TableModeAppend
	}
}

//+kubebuilder:webhook:path=/validate-ducklake-featherman-dev-v1alpha1-ducklaketable,mutating=false,failurePolicy=fail,sideEffects=None,groups=ducklake.featherman.dev,resources=ducklaketables,verbs=create;update,versions=v1alpha1,name=vducklaketable.kb.io,admissionReviewVersions=v1

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateCreate() (admission.Warnings, error) {
	ducklaketablelog.Info("validate create", "name", r.Name)

	var allErrs field.ErrorList

	// Validate table name format
	if !isValidTableName(r.Spec.Name) {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec").Child("name"),
			r.Spec.Name,
			"must start with a letter and contain only letters, numbers, and underscores",
		))
	}

	// Validate columns
	if len(r.Spec.Columns) == 0 {
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
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec").Child("format").Child("partitioning"),
				partition,
				"partition column must exist in table schema",
			))
		}
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeTable"},
		r.Name, allErrs)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	ducklaketablelog.Info("validate update", "name", r.Name)

	var allErrs field.ErrorList

	// Validate immutable fields
	oldTable := old.(*DuckLakeTable)
	if oldTable.Spec.Name != r.Spec.Name {
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
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec").Child("columns").Index(i).Child("type"),
					"column type is immutable",
				))
			}
		}
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "DuckLakeTable"},
		r.Name, allErrs)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (r *DuckLakeTable) ValidateDelete() (admission.Warnings, error) {
	ducklaketablelog.Info("validate delete", "name", r.Name)
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
