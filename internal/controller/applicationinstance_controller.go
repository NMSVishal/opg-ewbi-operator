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
	"time"

	opgmodels "github.com/neonephos-katalis/opg-ewbi-api/api/federation/models"
	"github.com/neonephos-katalis/opg-ewbi-operator/api/v1beta1"
	opgewbiv1beta1 "github.com/neonephos-katalis/opg-ewbi-operator/api/v1beta1"
	"github.com/neonephos-katalis/opg-ewbi-operator/internal/opg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ApplicationInstanceReconciler reconciles a ApplicationInstance object
type ApplicationInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	opg.OPGClientsMapInterface
	retryCounter sync.Map `json:"-"` // key: NamespacedName, value: int

}

var retryLimit int = 6
var guestRetryTime int = 20 // seconds
var hostRetryTime int = 20  // seconds

var initOnce sync.Once

func (r *ApplicationInstanceReconciler) init(ctx context.Context,
	req ctrl.Request) {
	log := log.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)

	log.Info("Initializing ApplicationInstanceReconciler settings from environment variables")
	val := os.Getenv("RETRY_LIMIT")
	if val == "" {
		log.Info("RETRY_LIMIT not set, using default 6")
		retryLimit = 6 // default if env not set
	} else {
		limit, err := strconv.Atoi(val)
		if err != nil {
			log.Info("Invalid RETRY_LIMIT value, using default 6")
			retryLimit = 6
		} else {
			retryLimit = limit
		}
	}
	// set guestRetryTime from env if exists
	guestVal := os.Getenv("GUEST_RETRY_TIME")
	if guestVal != "" {
		time, err := strconv.Atoi(guestVal)
		if err != nil {
			log.Info("Invalid GUEST_RETRY_TIME value, using default 10 seconds")
		} else {
			log.Info("Setting guestRetryTime from env", "value", time)
			guestRetryTime = time
		}
	}
	// set hostRetryTime from env if exists
	hostVal := os.Getenv("HOST_RETRY_TIME")
	if hostVal != "" {
		time, err := strconv.Atoi(hostVal)
		if err != nil {
			log.Info("Invalid HOST_RETRY_TIME value, using default 60 seconds")
		} else {
			log.Info("Setting hostRetryTime from env", "value", time)
			hostRetryTime = time
		}
	}

	fmt.Printf("Initialized env vars: RETRY_LIMIT=%d, GUEST_RETRY_TIME=%d, HOST_RETRY_TIME=%d\n",
		retryLimit, guestRetryTime, hostRetryTime)
}

// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applicationinstances,verbs=*,namespace=foo
// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applicationinstances/status,verbs=get;update;patch,namespace=foo
// +kubebuilder:rbac:groups=opg.ewbi.nby.one,resources=applicationinstances/finalizers,verbs=update,namespace=foo

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// Modify the Reconcile function to compare the state specified by
// the ApplicationInstance object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *ApplicationInstanceReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)
	log.Info("starting reconcile function for appInst")
	defer log.Info("end reconcile for appInst")

	// Getting main appInst or requeue
	var a v1beta1.ApplicationInstance
	if err := r.Get(ctx, req.NamespacedName, &a); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("appInst object not found")
			return ctrl.Result{}, nil
		}
		log.Error(err, "error getting appInst object")
		return ctrl.Result{}, err
	}

	// Getting appInst's federation or requeue by using federation-context-id label
	// extraLabels := map[string]string{v1beta1.FederationRelationLabel: a.Labels[v1beta1.FederationRelationLabel]}
	isGuest := IsGuestResource(a.Labels)
	extraLabels := map[string]string{}
	if isGuest {
		extraLabels[v1beta1.FederationRelationLabel] = string(v1beta1.FederationRelationGuest)
	} else {
		extraLabels[v1beta1.FederationRelationLabel] = string(v1beta1.FederationRelationHost)
	}
	feder, err := GetFederationByContextId(ctx, r.Client, a.Labels[v1beta1.FederationContextIdLabel], extraLabels)
	if err != nil {
		log.Error(err, "An ApplicattionInstance should always have a parent federation")
		a.Status.Phase = v1beta1.ApplicationInstancePhaseError
		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, errorUpdatingResourceStatusMsg)
		}
		return ctrl.Result{}, err
	}

	log.Info("Federation object obtained", "name", feder.Name)

	if a.GetDeletionTimestamp().IsZero() {
		if controllerutil.AddFinalizer(&a, v1beta1.ApplicationInstanceFinalizer) {
			log.Info("Added finalizer to appInst")
			if err := r.Update(ctx, a.DeepCopy()); err != nil {
				log.Info("unable to Update appInst with finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Successfully added finalizer to appInst")
			return ctrl.Result{}, nil
		}
	} else {
		if isGuest {
			if err := r.handleExternalAppInstDeletion(ctx, &a, feder); err != nil {
				log.Error(err, "error deleting appInst")
				a.Status.Phase = v1beta1.ApplicationInstancePhaseError
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{}, err
			}
		}
		// if external appInst is correctly deleted, we can remove the finalizer
		if controllerutil.RemoveFinalizer(&a, v1beta1.ApplicationInstanceFinalizer) {
			log.Info("Removed basic finalizer for appInst")
			if err := r.Update(ctx, a.DeepCopy()); err != nil {
				log.Error(err, "update failed while removing finalizers")
				return ctrl.Result{}, err
			}
			log.Info("removed all finalizers, exiting...")
			return ctrl.Result{}, nil
		}
	}
	initOnce.Do(func() {
		r.init(ctx, req)
	})

	//r.init(ctx, req)
	// Creation or Update Event Handling
	//if a.Labels[v1beta1.CreateEventLabel] == "true" {
	if a.Status.State == "" {
		log.Info("AppInst Creation Event Occured, reque after handling ", "retryAfter", time.Duration(guestRetryTime))
		if isGuest {
			if err := r.handleExternalAppInstCreation(ctx, &a, feder); err != nil {
				log.Info("error creating appInst")
				a.Status.Phase = v1beta1.ApplicationInstancePhaseError
				// update label create-event to false to avoid reprocessing
				a.Labels[v1beta1.CreateEventLabel] = "false"
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}
			return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil // Guest will requeue to check app instance status later
		} else {
			a.Status.Phase = opgewbiv1beta1.ApplicationInstancePhase(v1beta1.ApplicationInstanceStatePending)
			a.Status.State = v1beta1.ApplicationInstanceStatePending
			// assign empty AccessPointInfo struct
			a.Status.AccessPointInfo = v1beta1.AccessPointInfo{}
			a.Labels[v1beta1.CreateEventLabel] = "false"
			upErr := r.Status().Update(ctx, a.DeepCopy())
			if upErr != nil {
				log.Info("Error updating resource status", "appInst", a.Name)
				log.Error(upErr, errorUpdatingResourceStatusMsg)
			}
			//return ctrl.Result{}, nil
			return ctrl.Result{RequeueAfter: time.Duration(hostRetryTime) * time.Second}, nil // Host will requeue to manage internal appInst later
		}
	} else {
		log.Info("AppInst Update Event Occured")
		if isGuest {
			log.Info("Handling guest appInst status check, reque after handling ", "retryAfter", time.Duration(guestRetryTime), "retryLimit", retryLimit)
			if err := r.handleExternalAppInstStatusCheck(ctx, &a, feder); err != nil {
				log.Info("error creating appInst")
				a.Status.Phase = v1beta1.ApplicationInstancePhaseError
				//a.Status.State = v1beta1.ApplicationInstanceStateError
				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, errorUpdatingResourceStatusMsg)
				}
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}

			// Get current count
			val, _ := r.retryCounter.LoadOrStore(req.NamespacedName.String(), 0)
			count := val.(int)
			if count >= retryLimit && a.Status.State != v1beta1.ApplicationInstanceStateReady {
				// Stop retrying after 3 attempts
				log.Info("Retry limit reached for", "application instance CR ", req.NamespacedName)
				a.Status.State = v1beta1.ApplicationInstanceStateFailed
				a.Status.Phase = v1beta1.ApplicationInstancePhaseError

				upErr := r.Status().Update(ctx, a.DeepCopy())
				if upErr != nil {
					log.Error(upErr, "Error updating resource status", "appInst", a.Name)
					return ctrl.Result{}, upErr
				}
				return ctrl.Result{}, nil
			}
			r.retryCounter.Store(req.NamespacedName.String(), count+1)
			// check if status is Ready return withour requeue and else requeue
			if a.Status.State != v1beta1.ApplicationInstanceStateReady {
				log.Info("## Status not ready, requeuing", "current state", a.Status.State)
				//return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
				return ctrl.Result{RequeueAfter: time.Duration(guestRetryTime) * time.Second}, nil
			}
			return ctrl.Result{}, nil
		} else {
			log.Info("Handling host appInst status management, reque after handling ", "retryAfter", time.Duration(hostRetryTime), "retryLimit", 3)
			// Host operator will manage internal appInst state here
			// For now, we just set it to Ready ,Later it will be like pod creation checking
			val, _ := r.retryCounter.LoadOrStore(req.NamespacedName.String(), 0)
			count := val.(int)
			if count <= 3 && a.Status.State != v1beta1.ApplicationInstanceStateReady {
				log.Info("## Status not ready, requeuing", "current retry count", count)
				r.retryCounter.Store(req.NamespacedName.String(), count+1)
				return ctrl.Result{RequeueAfter: time.Duration(hostRetryTime) * time.Second}, nil
			}

			a.Status.Phase = opgewbiv1beta1.ApplicationInstancePhase(v1beta1.ApplicationInstanceStateReady)
			a.Status.State = v1beta1.ApplicationInstanceStateReady

			a.Status.AccessPointInfo = v1beta1.AccessPointInfo{
				InterfaceId: "test",
				AccessPoint: v1beta1.AccessPoint{
					Port:          8080,
					Fqdn:          "example.app.instance.local",
					Ipv4Addresses: "34.154.251.179",
				},
			}
			log.Info("Updating status", "AccessPointInfo", a.Status.AccessPointInfo)
			upErr := r.Status().Update(ctx, a.DeepCopy())
			if upErr != nil {
				log.Info("Error updating resource status", "appInst", a.Name)
				log.Error(upErr, errorUpdatingResourceStatusMsg)
			}
			log.Info("## Host appInst is ready, no requeue needed")
			return ctrl.Result{}, nil
		}

	}
	//return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ApplicationInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&opgewbiv1beta1.ApplicationInstance{}).
		Named("applicationinstance").
		Complete(r)
}

func (r *ApplicationInstanceReconciler) handleExternalAppInstCreation(
	ctx context.Context, a *v1beta1.ApplicationInstance, feder *v1beta1.Federation,
) error {
	log := log.FromContext(ctx)

	zone := struct {
		FlavourId           string                                                   `json:"flavourId"`
		ResPool             *string                                                  `json:"resPool,omitempty"`
		ResourceConsumption *opgmodels.InstallAppJSONBodyZoneInfoResourceConsumption `json:"resourceConsumption,omitempty"`
		ZoneId              string                                                   `json:"zoneId"`
	}{
		FlavourId:           a.Spec.ZoneInfo.FlavourId,
		ResPool:             &a.Spec.ZoneInfo.ResPool,
		ResourceConsumption: (*opgmodels.InstallAppJSONBodyZoneInfoResourceConsumption)(&a.Spec.ZoneInfo.ResourceConsumption),
		ZoneId:              a.Spec.ZoneInfo.ZoneId,
	}

	reqBody := opgmodels.InstallAppJSONRequestBody{
		AppId:               a.Spec.AppId,
		AppInstCallbackLink: a.Spec.CallbBackLink,
		AppInstanceId:       a.Labels[v1beta1.ExternalIdLabel],
		AppProviderId:       a.Spec.AppProviderId,
		AppVersion:          a.Spec.AppVersion,
		ZoneInfo:            zone,
	}

	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).InstallAppWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		reqBody)

	if err != nil {
		log.Error(err, "error creating appInst")
		return err
	}

	statusCode := res.StatusCode()

	switch {
	case statusCode >= 200 && statusCode < 300:
		log.Info("Created", "response", res)

		a.Status.Phase = v1beta1.ApplicationInstancePhaseReconciling
		a.Status.State = v1beta1.ApplicationInstanceStatePending

		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, "Error Updating resource", "appInst", a.Name)
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
		if *res.ApplicationproblemJSON500.Detail == "application not found" {
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

func (r *ApplicationInstanceReconciler) handleExternalAppInstDeletion(
	ctx context.Context, appInst *v1beta1.ApplicationInstance, feder *v1beta1.Federation,
) error {
	log := log.FromContext(ctx)
	log.Info("Deleting external appInst")
	// we should delete the appInst
	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).RemoveAppWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		appInst.Spec.AppId,
		appInst.Labels[v1beta1.ExternalIdLabel],
		appInst.Spec.ZoneInfo.ZoneId,
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

func (r *ApplicationInstanceReconciler) handleExternalAppInstStatusCheck(
	ctx context.Context, a *v1beta1.ApplicationInstance, feder *v1beta1.Federation,
) error {
	log := log.FromContext(ctx)
	log.Info("Checking external appInst status")

	// call here GetAppInstanceDetails(c echo.Context, federationContextId models.FederationContextId, appId models.AppIdentifier, appInstanceId models.InstanceIdentifier, zoneId models.ZoneIdentifier)
	res, err := r.GetOPGClient(
		feder.Labels[v1beta1.ExternalIdLabel],
		feder.Spec.GuestPartnerCredentials.TokenUrl,
		feder.Spec.GuestPartnerCredentials.ClientId,
	).GetAppInstanceDetailsWithResponse(
		context.TODO(),
		feder.Status.FederationContextId,
		a.Spec.AppId,
		a.Labels[v1beta1.ExternalIdLabel],
		a.Spec.ZoneInfo.ZoneId,
	)
	if err != nil {
		log.Error(err, "error checking appInst status")
		return err
	}

	statusCode := res.StatusCode()
	switch {
	case statusCode >= 200 && statusCode < 300:
		log.Info("AppInst status response", "response", res)
		log.Info("Successfully retrieved appInst status into string", "response", string(res.Body))

		var responseBody map[string]interface{}
		if err := json.Unmarshal([]byte(string(res.Body)), &responseBody); err != nil {
			log.Error(err, "Failed to unmarshal response body")
			return err
		}
		log.Info("Successfully retrieved appInst status into map", "response", responseBody)

		log.Info("## response type in response", "type", fmt.Sprintf("%T", responseBody["response"]))

		// Check for accesspointInfo
		rawAccessPointInfo, accessPointInfoExists := responseBody["accesspointInfo"]
		log.Info("Checking accesspointInfo", "exists", accessPointInfoExists, "type", fmt.Sprintf("%T", rawAccessPointInfo))
		if accessPointInfoArr, ok := rawAccessPointInfo.([]interface{}); ok && len(accessPointInfoArr) > 0 {
			log.Info("## Found accessPointInfo in response")
			var apiAccessPointInfo v1beta1.AccessPointInfo
			if firstAccessPointInfo, ok := accessPointInfoArr[0].(map[string]interface{}); ok {
				apiAccessPointInfo.InterfaceId, _ = firstAccessPointInfo["interfaceId"].(string)
				if accessPoint, ok := firstAccessPointInfo["accessPoints"].(map[string]interface{}); ok {
					var apiAccessPoint v1beta1.AccessPoint
					if port, ok := accessPoint["port"].(float64); ok {
						apiAccessPoint.Port = int(port)
					}
					apiAccessPoint.Fqdn, _ = accessPoint["fqdn"].(string)
					if ipv4Arr, ok := accessPoint["ipv4Addresses"].([]interface{}); ok && len(ipv4Arr) > 0 {
						if firstIpv4, ok := ipv4Arr[0].(string); ok {
							apiAccessPoint.Ipv4Addresses = firstIpv4
						}
					}
					if ipv6Arr, ok := accessPoint["ipv6Addresses"].([]interface{}); ok && len(ipv6Arr) > 0 {
						if firstIpv6, ok := ipv6Arr[0].(string); ok {
							apiAccessPoint.Ipv6Addresses = firstIpv6
						}
					}
					apiAccessPointInfo.AccessPoint = apiAccessPoint
				}
				a.Status.AccessPointInfo = apiAccessPointInfo
			}
		} else {
			log.Info("## accessPointInfo not found or empty in response", "type", fmt.Sprintf("%T", rawAccessPointInfo))
		}

		// Check for appInstanceState
		rawAppInstanceState, appInstanceStateExists := responseBody["appInstanceState"]
		log.Info("Checking appInstanceState", "exists", appInstanceStateExists, "type", fmt.Sprintf("%T", rawAppInstanceState))
		if state, ok := rawAppInstanceState.(string); ok {
			log.Info("## Found state in response", "state", state)
			a.Status.State = v1beta1.ApplicationInstanceState(state)
			switch a.Status.State {
			case v1beta1.ApplicationInstanceStateReady:
				a.Status.Phase = v1beta1.ApplicationInstancePhaseReady
			case v1beta1.ApplicationInstanceStatePending:
				a.Status.Phase = v1beta1.ApplicationInstancePhaseReconciling
			case v1beta1.ApplicationInstanceStateFailed:
				a.Status.Phase = v1beta1.ApplicationInstancePhaseError
			case v1beta1.ApplicationInstanceStateTerminating:
				a.Status.Phase = v1beta1.ApplicationInstancePhaseUnknown
			default:
				a.Status.Phase = v1beta1.ApplicationInstancePhaseUnknown
			}
		} else {
			log.Info("## appInstanceState not found in response", "type", fmt.Sprintf("%T", rawAppInstanceState))
		}

		log.Info("Updated AppInst status", "AccessPointInfo", a.Status.AccessPointInfo, "State", a.Status.State, "Phase", a.Status.Phase)

		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, "Error updating resource status", "appInst", a.Name)
			return upErr
		}
	case statusCode == 404:
		log.Info("AppInst not found in external system")
		a.Status.Phase = v1beta1.ApplicationInstancePhaseError
		upErr := r.Status().Update(ctx, a.DeepCopy())
		if upErr != nil {
			log.Error(upErr, "Error updating resource status", "appInst", a.Name)
			return upErr
		}
	default:
		log.Info("Unexpected status code", "status", statusCode, "body", string(res.Body))
	}

	return nil
}
