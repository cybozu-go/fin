package cmd

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"slices"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/controller"
	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	finmetrics "github.com/cybozu-go/fin/internal/pkg/metrics"
	webhookv1 "github.com/cybozu-go/fin/internal/webhook/v1"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(finv1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func controllerMain(args []string) error {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var rawImgExpansionUnitSize uint64
	var webhookCertPath string
	var webhookKeyPath string
	var overwriteFBCSchedule string

	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	fs.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	fs.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.BoolVar(&secureMetrics, "metrics-secure", false,
		"If set the metrics endpoint is served securely")
	fs.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	fs.Uint64Var(&rawImgExpansionUnitSize, "raw-img-expansion-unit-size", ceph.DefaultExpansionUnitSize,
		fmt.Sprintf("Set %s in jobs.", controller.EnvRawImgExpansionUnitSize))
	fs.StringVar(&webhookCertPath, "webhook-cert-path", "",
		"The file path of the webhook certificate file.")
	fs.StringVar(&webhookKeyPath, "webhook-key-path", "",
		"The file path of the webhook key file.")
	fs.StringVar(&overwriteFBCSchedule, "overwrite-fbc-schedule", "",
		"By setting this option, every CronJob created by this controller for every FinBackupConfig "+
			"will use its value as .spec.schedule. This option is intended for testing purposes only.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(fs)
	err := fs.Parse(args)
	if err != nil {
		return fmt.Errorf("unable to parse flags: %w", err)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	enableWebhook := os.Getenv("ENABLE_WEBHOOKS") != "false"
	setupLog.Info("ENABLE_WEBHOOKS evaluated", "env", os.Getenv("ENABLE_WEBHOOKS"), "enabled", enableWebhook)
	if enableWebhook {
		if webhookCertPath == "" || webhookKeyPath == "" {
			return fmt.Errorf("--webhook-cert-path and --webhook-key-path must be provided when webhooks are enabled")
		}
	}
	if rawImgExpansionUnitSize == 0 {
		return fmt.Errorf("raw-img-expansion-unit-size must be greater than 0")
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	mgrOptions := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "07c0f9a3.cybozu.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	}

	var webhookCertWatcher *certwatcher.CertWatcher

	if enableWebhook {
		webhookTLSOpts := slices.Clone(tlsOpts)
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-key-path", webhookKeyPath)

		webhookCertWatcher, err = certwatcher.New(webhookCertPath, webhookKeyPath)
		if err != nil {
			return fmt.Errorf("failed to initialize webhook certificate watcher: %w", err)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})

		mgrOptions.WebhookServer = webhook.NewServer(webhook.Options{
			TLSOpts: webhookTLSOpts,
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	// Register custom metrics
	finmetrics.Register()

	snapRepo := ceph.NewRBDRepository()
	maxPartSize, err := resource.ParseQuantity(os.Getenv("MAX_PART_SIZE"))
	if err != nil {
		return fmt.Errorf("failed to parse MAX_PART_SIZE environment variable: %w", err)
	}
	finBackupReconciler := controller.NewFinBackupReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		os.Getenv("POD_NAMESPACE"),
		os.Getenv("POD_IMAGE"),
		&maxPartSize,
		snapRepo,
		rawImgExpansionUnitSize,
	)
	if err = finBackupReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create controller FinBackup: %w", err)
	}

	rawImageChunkSize, err := resource.ParseQuantity(os.Getenv("RAW_IMAGE_CHUNK_SIZE"))
	if err != nil {
		return fmt.Errorf("failed to parse RAW_IMAGE_CHUNK_SIZE environment variable: %w", err)
	}
	finRestoreReconciler := controller.NewFinRestoreReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		os.Getenv("POD_NAMESPACE"),
		os.Getenv("POD_IMAGE"),
		&rawImageChunkSize,
	)
	if err = finRestoreReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create controller FinRestore: %w", err)
	}

	finBackupConfigReconciler := controller.NewFinBackupConfigReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		overwriteFBCSchedule,
		os.Getenv("POD_NAMESPACE"),
		os.Getenv("POD_IMAGE"),
		os.Getenv("CREATE_FINBACKUP_JOB_SERVICE_ACCOUNT"),
	)
	if err := finBackupConfigReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create controller FinBackupConfig: %w", err)
	}
	setupLog.Info("FinBackupConfig controller enabled")

	if enableWebhook {
		if err := webhookv1.SetupFinBackupWebhookWithManager(mgr); err != nil {
			return fmt.Errorf("unable to create webhook FinBackup: %w", err)
		}
		setupLog.Info("FinBackup validating webhook enabled")

		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			return fmt.Errorf("unable to add webhook certificate watcher to manager: %w", err)
		}
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}

	return nil
}
