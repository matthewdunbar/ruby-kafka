# frozen_string_literal: true

describe KafkaLegacy::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:broker_pool) { double(:broker_pool) }

    let(:cluster) {
      KafkaLegacy::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = KafkaLegacy::Protocol::MetadataResponse.new(
        brokers: [
          KafkaLegacy::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          KafkaLegacy::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              KafkaLegacy::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,
                partition_error_code: 5, # <-- this is the important bit.
              )
            ]
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error KafkaLegacy::LeaderNotAvailable
    end

    it "raises InvalidTopic if the topic is invalid" do
      metadata = KafkaLegacy::Protocol::MetadataResponse.new(
        brokers: [
          KafkaLegacy::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          KafkaLegacy::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            topic_error_code: 17, # <-- this is the important bit.
            partitions: []
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error KafkaLegacy::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = KafkaLegacy::Cluster.new(
        seed_brokers: [URI("kafka://not-there:9092"), URI("kafka://not-here:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )

      allow(broker_pool).to receive(:connect).and_raise(KafkaLegacy::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(KafkaLegacy::ConnectionError)
    end
  end
end
