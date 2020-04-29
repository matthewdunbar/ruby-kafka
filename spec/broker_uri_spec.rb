# frozen_string_literal: true

describe KafkaLegacy::BrokerUri do
  it "accepts valid seed brokers URIs" do
    expect(KafkaLegacy::BrokerUri.parse("kafka://hello").to_s).to eq "kafka://hello:9092"
    expect(KafkaLegacy::BrokerUri.parse("kafka+ssl://hello").to_s).to eq "kafka+ssl://hello:9092"
  end

  it "maps plaintext:// to kafka://" do
    expect(KafkaLegacy::BrokerUri.parse("PLAINTEXT://kafka").scheme).to eq "kafka"
  end

  it "maps ssl:// to kafka+ssl://" do
    expect(KafkaLegacy::BrokerUri.parse("SSL://kafka").scheme).to eq "kafka+ssl"
  end

  it "raises KafkaLegacy::Error on invalid schemes" do
    expect {
      KafkaLegacy::BrokerUri.parse("http://kafka")
    }.to raise_exception(KafkaLegacy::Error, "invalid protocol `http` in `http://kafka`")
  end
end
