package tests

import (
	"fmt"
	"time"

	kafkav1beta2 "github.com/RedHatInsights/strimzi-client-go/apis/kafka.strimzi.io/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	operatorconfig "github.com/stolostron/multicluster-global-hub/operator/pkg/config"
	migrationv1alpha1 "github.com/stolostron/multicluster-global-hub/operator/api/migration/v1alpha1"
	"github.com/stolostron/multicluster-global-hub/pkg/constants"
	"github.com/stolostron/multicluster-global-hub/pkg/transport"
	e2eutils "github.com/stolostron/multicluster-global-hub/test/e2e/utils"
)

var _ = Describe("Transport Migration Topic E2E", Label("e2e-test-transport-migration-topic"), Ordered, func() {
	var (
		sourceHubName   string
		targetHubName   string
		sourceHubClient client.Client
		targetHubClient client.Client
		migrationTopic  string
	)

	BeforeAll(func() {
		Expect(len(managedHubNames)).To(BeNumerically(">=", 2))
		sourceHubName = managedHubNames[0]
		targetHubName = managedHubNames[1]
		migrationTopic = operatorconfig.GetMigrationTopic()

		var err error
		sourceHubClient, err = testClients.RuntimeClient(sourceHubName, agentScheme)
		Expect(err).NotTo(HaveOccurred())
		targetHubClient, err = testClients.RuntimeClient(targetHubName, agentScheme)
		Expect(err).NotTo(HaveOccurred())
	})

	Context("ACM-34442 Phase 3 - dedicated gh-migration topic", func() {
		It("should provision the gh-migration KafkaTopic in the global hub namespace", func() {
			topic := &kafkav1beta2.KafkaTopic{}
			Eventually(func() error {
				return globalHubClient.Get(ctx, types.NamespacedName{
					Name:      migrationTopic,
					Namespace: testOptions.GlobalHub.Namespace,
				}, topic)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
			Expect(topic.Spec.Partitions).To(BeNumerically(">", 0))
		})

		It("should include the migration topic in managed hub transport credentials", func() {
			secret := &corev1.Secret{}
			Expect(sourceHubClient.Get(ctx, types.NamespacedName{
				Name:      constants.GHTransportSecretName,
				Namespace: constants.GHAgentNamespace,
			}, secret)).To(Succeed())

			kafkaYAML, ok := secret.Data["kafka.yaml"]
			Expect(ok).To(BeTrue(), "transport secret must contain kafka.yaml")

			kafkaConfig := &transport.KafkaConfig{}
			Expect(yaml.Unmarshal(kafkaYAML, kafkaConfig)).To(Succeed())
			Expect(kafkaConfig.MigrationTopic).To(Equal(migrationTopic))
		})
	})

	Context("Migration deploying over gh-migration", func() {
		var (
			publisher        *e2eutils.KafkaEventPublisher
			trustedMigration string
			spoofMigrationNS string
			testClusterName  string
		)

		BeforeEach(func() {
			trustedMigration = fmt.Sprintf("%s-trusted-%d", spoofMigrationNSPrefix, time.Now().UnixNano())
			spoofMigrationNS = fmt.Sprintf("%s-spoof-%d", spoofMigrationNSPrefix, time.Now().UnixNano())
			Expect(len(managedClusterNames)).To(BeNumerically(">=", 1))
			testClusterName = managedClusterNames[0]

			var err error
			publisher, err = e2eutils.NewKafkaEventPublisher(sourceHubClient, constants.GHAgentNamespace)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = targetHubClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: trustedMigration}})
			_ = targetHubClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: spoofMigrationNS}})
			_ = targetHubClient.Delete(ctx, &migrationv1alpha1.ManagedClusterMigration{
				ObjectMeta: metav1.ObjectMeta{Name: spoofMigrationID, Namespace: constants.GHDefaultNamespace},
			})
		})

		It("should apply deploying migration resources received on gh-migration from the registered source hub", func() {
			ensureDeployingMigrationCR(ctx, targetHubClient, sourceHubName, targetHubName, testClusterName)

			evt := migrationDeployingEvent(sourceHubName, targetHubName, trustedMigration, testClusterName)
			Expect(publisher.SendToTopic(ctx, publisher.MigrationTopic(), evt)).To(Succeed())

			Eventually(func() error {
				ns := &corev1.Namespace{}
				return targetHubClient.Get(ctx, types.NamespacedName{Name: trustedMigration}, ns)
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should drop migration deploying events on gh-migration from an untrusted source hub", func() {
			ensureDeployingMigrationCR(ctx, targetHubClient, sourceHubName, targetHubName, testClusterName)

			evt := migrationDeployingEvent(spoofMigrationSource, targetHubName, spoofMigrationNS, testClusterName)
			Expect(publisher.SendToTopic(ctx, publisher.MigrationTopic(), evt)).To(Succeed())

			Consistently(func() error {
				ns := &corev1.Namespace{}
				err := targetHubClient.Get(ctx, types.NamespacedName{Name: spoofMigrationNS}, ns)
				if err == nil {
					return fmt.Errorf("namespace %q must not be created from spoofed migration source on %s",
						spoofMigrationNS, migrationTopic)
				}
				return client.IgnoreNotFound(err)
			}, 45*time.Second, 500*time.Millisecond).Should(Succeed())
		})
	})
})
