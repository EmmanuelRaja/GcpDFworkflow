package DemoProjects.demo.src.main.GCSToBq;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;

public void publishMessage(String projectId, String topicId, String message) throws Exception {
    Publisher publisher = Publisher.newBuilder(TopicName.of(projectId, topicId)).build();
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build();
    publisher.publish(pubsubMessage);
    publisher.shutdown();
}
