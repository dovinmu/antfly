package v1

import (
	"context"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/validate-antfly-io-v1-antflyserverlessproject,mutating=false,failurePolicy=fail,sideEffects=None,groups=antfly.io,resources=antflyserverlessprojects,verbs=create;update,versions=v1,name=vantflyserverlessproject.kb.io,admissionReviewVersions=v1

// AntflyServerlessProjectValidator implements admission.Validator for AntflyServerlessProject.
type AntflyServerlessProjectValidator struct{}

var _ admission.Validator[*antflyv1.AntflyServerlessProject] = &AntflyServerlessProjectValidator{}

func (v *AntflyServerlessProjectValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyServerlessProject) (admission.Warnings, error) {
	return nil, obj.ValidateAntflyServerlessProject()
}

func (v *AntflyServerlessProjectValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyServerlessProject) (admission.Warnings, error) {
	_ = oldObj
	return nil, newObj.ValidateAntflyServerlessProject()
}

func (v *AntflyServerlessProjectValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyServerlessProject) (admission.Warnings, error) {
	_ = obj
	return nil, nil
}
