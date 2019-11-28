package org.plugin;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.streams.Stream;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class PubSubOutput implements MessageOutput {
    private boolean shutdown;
    private static final String CK_CREDENTIAL_FILE = "credential_file";
    private static final String CK_TOPIC_NAME = "topic_name";
    private static final String CK_CONFIG_HTTP_PROXY = "proxy url";
    private static Configuration config;
    private static final Logger LOG = LoggerFactory.getLogger(PubSubOutput.class);

    @Inject
    public PubSubOutput(@Assisted Configuration conf) {
        config = conf;
        shutdown = false;
    }

    @Override
    public boolean isRunning() {
        return !shutdown;
    }

    @Override
    public void stop() {
        shutdown = true;

    }

    @Override
    public void write(List<Message> msgs) {
        for (Message msg : msgs) {
            writeBuffer(msg.getFields());
        }
    }

    @Override
    public void write(Message msg) {
        if (shutdown) {
            return;
        }
        writeBuffer(msg.getFields());
    }

    private void writeBuffer(Map<String, Object> data) {


        Gson gson = new Gson();
        JSONObject obj = new JSONObject(data);
        Publisher publisher = null;

        DateTime dt = (DateTime) obj.get("timestamp");
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        String dtStr = fmt.print(dt);
        obj.put("timestamp", dtStr);
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();


        try {

            JsonReader credJson = new JsonReader(new FileReader(Objects.requireNonNull(config.getString(CK_CREDENTIAL_FILE))));
            Type stringMapType = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> credMap = gson.fromJson(credJson, stringMapType);
            String topicId = config.getString(CK_TOPIC_NAME);
            String projectId = credMap.get("project_id");
            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
//            System.setProperty("java.net.useSystemProxies", "true");

//            System.setProperty("http.proxyHost", "172.21.10.100");
//            System.setProperty("http.proxyPort", "80");
//            System.setProperty("https.proxyPort","80");

            try (InputStream credential = new FileInputStream(Objects.requireNonNull(config.getString(CK_CREDENTIAL_FILE)))) {
                CredentialsProvider credentialsProvider = FixedCredentialsProvider
                        .create(ServiceAccountCredentials.fromStream(credential));
                // endpoint can be set here
                publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider).build();

                ByteString finalData = ByteString.copyFromUtf8(String.valueOf(obj));
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(finalData)
                        .build();
                ApiFuture<String> future = publisher.publish(pubsubMessage);
                messageIdFutures.add(future);
//                ApiFutures.addCallback(
//                        future,
//                        new ApiFutureCallback<String>() {
//
//                            @Override
//                            public void onFailure(Throwable throwable) {
//                                if (throwable instanceof ApiException) {
//                                    ApiException apiException = ((ApiException) throwable);
//                                    // details on the API exception
//                                    System.out.println(apiException.getStatusCode().getCode());
//                                    System.out.println(apiException.isRetryable());
//                                }
//                                System.out.println("Error publishing message : " + String.valueOf(obj));
//                            }
//
//                            @Override
//                            public void onSuccess(String messageId) {
//                                // Once published, returns server-assigned message ids (unique within the topic)
//                                System.out.println(messageId);
//                            }
//                        },
//                        MoreExecutors.directExecutor());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (publisher != null) {
                try {
                    try {
                        publisher.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    publisher.awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public interface Factory extends MessageOutput.Factory<PubSubOutput> {
        @Override
        PubSubOutput create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("PubSub Output", false, "", "Forwards stream to PubSub.");
        }
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();
            configurationRequest.addField(new TextField(CK_CREDENTIAL_FILE, "Credential file location", "",
                    "A Path to the TLS private key file", ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(new TextField(CK_TOPIC_NAME, "Topic name", "",
                    "Topic where data is to be published", ConfigurationField.Optional.NOT_OPTIONAL));
//            configurationRequest.addField(new TextField(CK_CONFIG_HTTP_PROXY,
//                    "HTTP Proxy URI",
//                    "",
//                    "URI of HTTP Proxy to be used if required",
//                    ConfigurationField.Optional.OPTIONAL));
            return configurationRequest;
        }
    }
}
