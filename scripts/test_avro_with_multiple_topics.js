import { check } from "k6";
import { Writer, Reader, Connection } from "k6/x/kafka";

const brokers = ["localhost:9092"];
const topics = ["topicA", "topicB"];

// Create Kafka connection
const connection = new Connection({ address: brokers[0] });

// Create topics (only once for VU 0)
if (__VU === 0) {
  for (const topic of topics) {
    connection.createTopic({ topic });
  }
}

// Create writers and readers for each topic
const writers = new Map();
const readers = new Map();

topics.forEach((topic) => {
  writers.set(
    topic,
    new Writer({
      brokers,
      topic,
      autoCreateTopic: false,
    }),
  );

  readers.set(
    topic,
    new Reader({
      brokers,
      topic,
    }),
  );
});

export default function () {
  for (const topic of topics) {
    const writer = writers.get(topic);

    // Produce 5 messages to each topic
    const messages = Array.from({ length: 5 }, (_, i) => ({
      key: `key-${topic}-${i}`,
      value: `value-${topic}-${i}`,
    }));

    writer.produce({ messages });

    // Read back 5 messages from each topic
    const reader = readers.get(topic);
    const consumed = reader.consume({ limit: 5 });

    check(consumed, {
      [`${topic}: 5 messages read`]: (msgs) => msgs.length === 5,
      [`${topic}: first key correct`]: (msgs) =>
        msgs[0].key.includes(`key-${topic}`),
    });
  }
}

export function teardown() {
  if (__VU === 0) {
    for (const topic of topics) {
      connection.deleteTopic(topic);
    }
  }

  // Close all clients
  writers.forEach((w) => w.close());
  readers.forEach((r) => r.close());
  connection.close();
}
