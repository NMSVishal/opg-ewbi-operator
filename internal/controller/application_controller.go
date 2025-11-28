/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	opgmodels "github.com/neonephos-katalis/opg-ewbi-api/api/federation/models"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/neonephos-katalis/opg-ewbi-operator/api/v1beta1"
	opgewbiv1beta1 "github.com/neonephos-katalis/opg-ewbi-operator/api/v1beta1"
	"github.com/neonephos-katalis/opg-ewbi-operator/internal/opg"
)

const (
	errorUpdatingResourceStatusMsg = "Error Updating resource status"
	unexpectedStatusCodeMsg        = "Unexpected Status Code"
)

// ApplicationReconciler reconciles a Application object
type ApplicationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	opg.OPGClientsMapInterface
}

var applicationGuestRetryTime int = 60 // default 10 seconds
var initOne sync.Once

var updateCounter int32 = 0

func (r *ApplicationReconciler) init(ctx context.Context,
	req ctrl.Request) {
	log := log.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)

	log.Info("Initializing ApplicationReconciler settings from environment variables")

	// set applicationGuestRetryTime from env var if defined
	hostVal := os.Getenv("APPLICATION_GUEST_RETRY_TIME")
	if hostVal != "" {
		time, err := strconv.Atoi(hostVal)
		if err != nil {
			log.Info("Invalid APPLICATION_GUEST_RETRY_TIME value, using default 60 seconds")
		} else {
			log.Info("Setting hostRetryTime from env", "value", time)
			applicationGuestRetryTime = time
		}
	}
	fmt.Printf("Initialized env vars: APPLICATION_GUEST_RETRY_TIME=%d\n", applicationGuestRetryTime)
}

// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applications,verbs=*,namespace=foo
// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applications/status,verbs=get;update;patch,namespace=foo
// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applications/finalizers,verbs=update,namespace=foo

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Modify the Reconcile function to compare the state specified by
// the Application object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.4/pkg/reconcile
func (r *ApplicationReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)
	log.Info("starting reconcile function for app")
	defer log.Info("end reconcile for app")

	// Getting main app or requeue
	var a v1beta1.Application
	if err := r.Get(ctx, req.NamespacedName, &a); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("app object not found")
			return ctrl.Result{}, nil
		}
		log.Error(err, "error getting app object")
		return ctrl.Result{}, err
	}

	// Getting app's federation or requeue by using federation-context-id label
	isGuest := IsGuestResource(a.Labels)
	extraLabels := map[string]string{}
	if isGuest {
		extraLabels[v1beta1.FederationRelationLabel] = string(v1beta1.FederationRelationGuest)
	} else {
		extraLabels[v1beta1.FederationRelationLabel] = string(v1beta1.FederationRelationHost)
	}
	feder, err := GetFederationByContextId(ctx, r.Client, a.Labels[v1beta1.FederationContextIdLabel], extraLabels)
	if err != nil {
		log.Error(err, "An Applicattion should always have a parent federation")
		a.Status.Phase = v1beta1.ApplicationPhaseError
		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, errorUpdatingResourceStatusMsg)
		}
		return ctrl.Result{}, err
	}

	log.Info("Federation object obtained", "name", feder.Name)

	if a.GetDeletionTimestamp().IsZero() {
		if controllerutil.AddFinalizer(&a, v1beta1.AppFinalizer) {
			log.Info("Added finalizer to app")
			if err := r.Update(ctx, a.DeepCopy()); err != nil {
				log.Info("unable to Update app with finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Successfully added finalizer to app")
			return ctrl.Result{}, nil
		}
	} else {
		if isGuest {
			if err := r.handleExternalAppDeletion(ctx, &a, feder); err != nil {
				log.Error(err, "error deleting app")
				a.Status.Phase = v1beta1.ApplicationPhaseError
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{}, err
			}
		}
		// if external app is correctly deleted, we can remove the finalizer
		if controllerutil.RemoveFinalizer(&a, v1beta1.AppFinalizer) {
			log.Info("Removed basic finalizer for app")
			if err := r.Update(ctx, a.DeepCopy()); err != nil {
				log.Error(err, "update failed while removing finalizers")
				return ctrl.Result{}, err
			}
			log.Info("removed all finalizers, exiting...")
			return ctrl.Result{}, nil
		}
	}

	initOne.Do(func() {
		r.init(ctx, req)
	})

	// handle create call here
	if a.Status.Phase == "" {
		// if federation is guest, send OPG API request
		if isGuest {
			if err := r.handleExternalAppCreation(ctx, &a, feder); err != nil {
				log.Info("####error creating app")
				a.Status.Phase = v1beta1.ApplicationPhaseError
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}
			return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime)}, nil
		} else {
			a.Status.Phase = v1beta1.ApplicationPhaseReady
			//a.Status.Phase = v1beta1.ApplicationPhaseReconciling
			upErr := r.Status().Update(ctx, a.DeepCopy())
			if upErr != nil {
				log.Error(upErr, errorUpdatingResourceStatusMsg)
			}
			return ctrl.Result{}, nil
		}
	} else {
		log.Info(" Application update received")
		if isGuest {
			if atomic.LoadInt32(&updateCounter) == 0 {
				// First time: skip update
				atomic.StoreInt32(&updateCounter, 1)
				log.Info("#### Skipping  first update call ###")
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}
			log.Info(" Checking External app updates")
			if err := r.handleExternalAppUpdate(ctx, &a, feder); err != nil {
				log.Info("error updating app")
				a.Status.Phase = v1beta1.ApplicationPhaseError
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{}, nil
			}
			// check is status is reconciling , then requeue after guestRetryTime
			if a.Status.Phase == v1beta1.ApplicationPhaseReconciling {
				log.Info(" App is in reconciling state , Requeuing after guestRetryTime")
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}
			return ctrl.Result{}, nil

		} else {
			log.Info(" Internal app update received")
			if a.Status.Phase == "" {
				log.Info(" Received empty status.phase , Setting phase to Ready")
				a.Status.Phase = v1beta1.ApplicationPhaseReady
			}
			upErr := r.Status().Update(ctx, a.DeepCopy())
			if upErr != nil {
				log.Error(upErr, errorUpdatingResourceStatusMsg)
			}
			return ctrl.Result{}, nil
		}
	}
}

func (r *ApplicationReconciler) handleExternalAppUpdate(ctx context.Context, application *opgewbiv1beta1.Application, feder *opgewbiv1beta1.Federation) any {
	log := log.FromContext(ctx)
	log.Info("Fetching external app details")
	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).ViewApplicationWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		application.Labels[v1beta1.ExternalIdLabel],
	)
	if err != nil {
		log.Error(err, "error getting app")
		return err
	}

	statusCode := res.StatusCode()

	switch {
	case statusCode >= 200 && statusCode < 300:
		log.Info("Fetched external app details", "response", res)
		// Update application status based on fetched details if needed
		application.Status.Phase = v1beta1.ApplicationPhaseReady

		log.Info("Successfully retrieved application into string", "response", string(res.Body))

		var respBody map[string]interface{}
		if err := json.Unmarshal([]byte(string(res.Body)), &respBody); err != nil {
			log.Error(err, "Failed to unmarshal response body")
			return err
		}
		log.Info("Successfully retrieved application into map", "response", respBody)
		if appComponentSpecs, exists := respBody["appComponentSpecs"].([]interface{}); exists {
			var componentSpecs []opgewbiv1beta1.ComponentSpecRef
			for _, comp := range appComponentSpecs {
				if compMap, ok := comp.(map[string]interface{}); ok {
					if artefactId, ok := compMap["artefactId"].(string); ok {
						componentSpecs = append(componentSpecs, opgewbiv1beta1.ComponentSpecRef{
							ArtefactId: artefactId,
						})
					}
				}
			}
			application.Spec.ComponentSpecs = componentSpecs
		} else {
			log.Info("appComponentSpecs not found in response")
		}
		// Example: Update QoSProfile fields
		if appQoSProfile, exists := respBody["appQoSProfile"].(map[string]interface{}); exists {
			if latencyConstraints, ok := appQoSProfile["latencyConstraints"].(string); ok {
				application.Spec.QoSProfile.LatencyConstraints = string(latencyConstraints)
			}
			if multiUserClients, ok := appQoSProfile["multiUserClients"].(string); ok {
				application.Spec.QoSProfile.MultiUserClients = string(multiUserClients)
			}
			if noOfUsersPerAppInst, ok := appQoSProfile["noOfUsersPerAppInst"].(float64); ok {
				application.Spec.QoSProfile.UsersPerAppInst = int64(noOfUsersPerAppInst)
			}
			if appProvisioning, ok := appQoSProfile["appProvisioning"].(bool); ok {
				application.Spec.QoSProfile.Provisioning = appProvisioning
			}
		}

		if appMetaData, exists := respBody["appMetaData"].(map[string]interface{}); exists {
			if appName, ok := appMetaData["appName"].(string); ok {
				application.Spec.MetaData.Name = appName
			}
			if accessToken, ok := appMetaData["accessToken"].(string); ok {
				application.Spec.MetaData.AccessToken = accessToken
			}
			if mobilitySupport, ok := appMetaData["mobilitySupport"].(bool); ok {
				application.Spec.MetaData.MobilitySupport = mobilitySupport
			}
			if version, ok := appMetaData["version"].(string); ok {
				application.Spec.MetaData.Version = version
			}
		}

		if appProviderId, ok := respBody["appProviderId"].(string); ok {
			application.Spec.AppProviderId = appProviderId
		}

		upErr := r.Status().Update(ctx, application.DeepCopy())
		if upErr != nil {
			log.Error(upErr, "Error Updating resource", "app", application.Name)
			return upErr
		}
		log.Info("#####Application spec updated based on external app details")
	case statusCode == 400:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON400)
		log.Info("Couldn't fetch app", "Detail", res.ApplicationproblemJSON400.Detail)
		return errors.New(*res.ApplicationproblemJSON400.Detail)
	case statusCode == 401:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON401)
	case statusCode == 404:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON404)
	case statusCode == 409:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON409)
	case statusCode == 422:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON422)
	case statusCode == 500:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON500)
	case statusCode == 503:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON503)
	case statusCode == 520:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON520)
	default:
		log.Info(unexpectedStatusCodeMsg, "status", statusCode, "body", string(res.Body))
	}
	return nil

}

// SetupWithManager sets up the controller with the Manager.
func (r *ApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&opgewbiv1beta1.Application{}).
		Named("application").
		Complete(r)
}

func (r *ApplicationReconciler) handleExternalAppCreation(
	ctx context.Context, a *v1beta1.Application, feder *v1beta1.Federation,
) error {
	log := log.FromContext(ctx)
	numUsers := int(a.Spec.QoSProfile.UsersPerAppInst)
	multiUserClients := opgmodels.AppQoSProfileMultiUserClients(a.Spec.QoSProfile.MultiUserClients)
	components := opgmodels.AppComponentSpecs{}

	// opgmodels.AppComponentSpecs{} is a "[]struct"
	for _, c := range a.Spec.ComponentSpecs {

		newComponent := struct {
			ArtefactId    opgmodels.ArtefactId `json:"artefactId"`
			ComponentName *string              `json:"componentName,omitempty"`
			ServiceNameEW *string              `json:"serviceNameEW,omitempty"`
			ServiceNameNB *string              `json:"serviceNameNB,omitempty"`
		}{
			ArtefactId: c.ArtefactId,
		}
		components = append(components, newComponent)
	}

	appReqBody := opgmodels.OnboardApplicationJSONRequestBody{
		// AppDeploymentZones:    &[]opgmodels.ZoneIdentifier{},
		AppId: a.Labels[v1beta1.ExternalIdLabel],
		AppMetaData: opgmodels.AppMetaData{
			AccessToken: a.Spec.MetaData.AccessToken,
			// AppDescription:  new(string),
			AppName: a.Spec.MetaData.Name,
			// Category:        &"",
			MobilitySupport: &a.Spec.MetaData.MobilitySupport,
			Version:         a.Spec.MetaData.Version,
		},
		AppProviderId: a.Spec.AppProviderId,
		AppQoSProfile: opgmodels.AppQoSProfile{
			AppProvisioning: &a.Spec.QoSProfile.Provisioning,
			// BandwidthRequired:   new(int32),
			LatencyConstraints:  opgmodels.AppQoSProfileLatencyConstraints(a.Spec.QoSProfile.LatencyConstraints),
			MultiUserClients:    &multiUserClients,
			NoOfUsersPerAppInst: &numUsers,
		},
		AppStatusCallbackLink: a.Spec.StatusLink,
		AppComponentSpecs:     components,
	}

	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).OnboardApplicationWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		appReqBody)

	if err != nil {
		log.Error(err, "error creating app")
		return err
	}

	statusCode := res.StatusCode()

	switch {
	case statusCode >= 200 && statusCode < 300:
		log.Info("Created", "response", res)

		//a.Status.Phase = v1beta1.ApplicationPhaseReady
		a.Status.Phase = v1beta1.ApplicationPhaseReconciling
		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, "Error Updating resource", "app", a.Name)
			return upErr
		}

	case statusCode == 400:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON400)
		log.Info("Couldn't be created", "Detail", res.ApplicationproblemJSON400.Detail)
		return errors.New(*res.ApplicationproblemJSON400.Detail)
	case statusCode == 401:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON401)
	case statusCode == 404:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON404)
	case statusCode == 409:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON409)
	case statusCode == 422:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON422)
	case statusCode == 500:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON500)
		// this should be deleted when API returns a 400 for this case
		if *res.ApplicationproblemJSON500.Detail == "artefact not found" {
			return errors.New(*res.ApplicationproblemJSON500.Detail)
		}
	case statusCode == 503:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON503)
	case statusCode == 520:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON520)
	default:
		log.Info(unexpectedStatusCodeMsg, "status", statusCode, "body", string(res.Body))
	}
	return nil
}

func (r *ApplicationReconciler) handleExternalAppDeletion(
	ctx context.Context, a *v1beta1.Application, feder *v1beta1.Federation,
) error {
	log := log.FromContext(ctx)
	log.Info("Deleting external app")
	// we should delete the app
	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).DeleteAppWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		a.Labels[v1beta1.ExternalIdLabel],
	)
	if err != nil {
		log.Error(err, "error deleting federation")
		return err
	}

	statusCode := res.StatusCode()

	switch {
	case statusCode >= 200 && statusCode < 300:
		log.Info("Deleted")
		// federResponse.OfferedAvailabilityZones
	case statusCode == 400:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON400)
	case statusCode == 401:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON401)
	case statusCode == 404:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON404)
	case statusCode == 409:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON409)
	case statusCode == 422:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON422)
	case statusCode == 500:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON500)
	case statusCode == 503:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON503)
	case statusCode == 520:
		handleProblemDetails(log, statusCode, res.ApplicationproblemJSON520)
	default:
		log.Info(unexpectedStatusCodeMsg, "status", statusCode, "body", string(res.Body))
	}
	return nil
}
