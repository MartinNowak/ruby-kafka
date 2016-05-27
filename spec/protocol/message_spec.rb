describe Kafka::Protocol::Message do
  describe "#encode" do
    let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }

    it 'encodes messages' do
      msg = Kafka::Protocol::Message.new(value: nil)
      expect(Kafka::Protocol::Encoder.encode_with(msg).encode.length).to eq 26
      msg = Kafka::Protocol::Message.new(value: 'abcd')
      expect(Kafka::Protocol::Encoder.encode_with(msg).encode.length).to eq 30
    end

    it 'encodes new message format with timestamp' do
      msg = Kafka::Protocol::Message.new(value: nil, timestamp: 1)
      expect(Kafka::Protocol::Encoder.encode_with(msg).length).to eq 34
      msg = Kafka::Protocol::Message.new(value: 'abcd', timestamp: 1234)
      expect(Kafka::Protocol::Encoder.encode_with(msg).length).to eq 38
    end
  end

  describe ".decode" do
    def test_msg msg
      str = Kafka::Protocol::Encoder.encode_with(msg).encode
      decoded = Kafka::Protocol::Message.decode Kafka::Protocol::Decoder.from_string(str)
      expect(decoded).to eq msg
    end

    it 'decodes messages' do
      test_msg Kafka::Protocol::Message.new(value: nil)
      test_msg Kafka::Protocol::Message.new(key: 'key', value: 'value')
    end

    it 'decodes new message format with timestamp' do
      test_msg Kafka::Protocol::Message.new(value: nil, timestamp: 1234)
      test_msg Kafka::Protocol::Message.new(key: 'key', value: 'value', timestamp: 1234)
    end
  end
end
