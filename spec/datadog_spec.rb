# frozen_string_literal: true

require "kafka-legacy/datadog"
require "fake_datadog_agent"

describe KafkaLegacy::Datadog do
  let(:agent) { FakeDatadogAgent.new }

  before do
    agent.start
  end

  after do
    agent.stop
  end

  it "emits metrics to the Datadog agent" do
    KafkaLegacy::Datadog.host = agent.host
    KafkaLegacy::Datadog.port = agent.port

    client = KafkaLegacy::Datadog.statsd

    client.increment("greetings")

    agent.wait_for_metrics

    expect(agent.metrics.count).to eq 1

    metric = agent.metrics.first

    expect(metric).to eq "ruby_kafka.greetings"
  end
end
