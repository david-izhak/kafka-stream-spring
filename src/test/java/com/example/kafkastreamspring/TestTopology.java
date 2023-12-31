package com.example.kafkastreamspring;

import com.example.kafkastreamspring.config.KafkaConfiguration;
import com.example.kafkastreamspring.config.TopologyConfiguration;
import com.example.kafkastreamspring.model.MessageOutput;
import com.example.kafkastreamspring.model.MessageUserBalance;
import com.example.kafkastreamspring.model.MessageUserState;
import com.example.kafkastreamspring.util.StreamsSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Instant;
import java.util.NoSuchElementException;

import static com.example.kafkastreamspring.config.TopicNames.*;
import static com.example.kafkastreamspring.model.UserState.ACTIVE;
import static com.example.kafkastreamspring.model.UserState.BLOCKED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Test topology")
@Tag("topology")
public class TestTopology {

    private TestInputTopic<String, MessageUserState> userStateTestInputTopic;
    private TestInputTopic<String, MessageUserBalance> userBalanceTestInputTopic;
    private TestOutputTopic<String, MessageOutput> outputTopic;
    TopologyTestDriver topologyTestDriver;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(topology, config.asProperties());
        userStateTestInputTopic = topologyTestDriver.createInputTopic(USER_STATE_TOPIC, Serdes.String().serializer(),
                        StreamsSerdes.messageUserStateSerde().serializer());
        userBalanceTestInputTopic = topologyTestDriver.createInputTopic(USER_BALANCE_TOPIC,
                Serdes.String().serializer(),
                StreamsSerdes.messageUserBalanceSerde().serializer());
        outputTopic =
                topologyTestDriver.createOutputTopic(USER_COMBINED_VALUES_TOPIC, Serdes.String().deserializer(),
                        StreamsSerdes.messageOutputSerde().deserializer());
    }

    @AfterEach
    public void tearDown() {
        try {
            topologyTestDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test topology with one message for each input topic")
    @Tag("message")
    void testTopology() {
        //arrange

        MessageUserState messageUserState1 = MessageUserState.builder()
                .userId("abc")
                .state(ACTIVE)
                .timestamp(Instant.now().minusSeconds(10))
                .build();

        MessageUserBalance messageUserBalance1 = MessageUserBalance.builder()
                .userId("abc")
                .accountBalance(100.0)
                .timestamp(Instant.now().minusSeconds(9))
                .build();


        //act
        userStateTestInputTopic.pipeInput(messageUserState1.getUserId(), messageUserState1);

        userBalanceTestInputTopic.pipeInput(messageUserBalance1.getUserId(), messageUserBalance1);

        //assert
        KeyValue<String, MessageOutput> keyValue = outputTopic.readKeyValue();
        assertEquals(messageUserState1.getUserId(), keyValue.key);
        assertEquals(ACTIVE, keyValue.value.getState());
        assertEquals(100.0, keyValue.value.getAccountBalance());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("Test topology with two messages for each input topic")
    void testTopologyTwoOutputMessages() {
        //arrange

        MessageUserState messageUserState1 = MessageUserState.builder()
                .userId("abc")
                .state(ACTIVE)
                .timestamp(Instant.now().minusSeconds(10))
                .build();
        MessageUserState messageUserState2 = MessageUserState.builder()
                .userId("abc")
                .state(BLOCKED)
                .timestamp(Instant.now().plusSeconds(8))
                .build();

        MessageUserBalance messageUserBalance1 = MessageUserBalance.builder()
                .userId("abc")
                .accountBalance(100.0)
                .timestamp(Instant.now().minusSeconds(9))
                .build();

        MessageUserBalance messageUserBalance2 = MessageUserBalance.builder()
                .userId("abc")
                .accountBalance(200.0)
                .timestamp(Instant.now().plusSeconds(7))
                .build();

        //act
        userStateTestInputTopic.pipeInput(messageUserState1.getUserId(), messageUserState1);
        userStateTestInputTopic.pipeInput(messageUserState2.getUserId(), messageUserState2);

        userBalanceTestInputTopic.pipeInput(messageUserBalance1.getUserId(), messageUserBalance1);
        userBalanceTestInputTopic.pipeInput(messageUserBalance2.getUserId(), messageUserBalance2);

        //assert
        KeyValue<String, MessageOutput> keyValue = outputTopic.readKeyValue();
        assertEquals(messageUserState1.getUserId(), keyValue.key);
        assertEquals(BLOCKED, keyValue.value.getState());
        assertEquals(100.0, keyValue.value.getAccountBalance());

        KeyValue<String, MessageOutput> keyValue2 = outputTopic.readKeyValue();
        assertEquals(messageUserState1.getUserId(), keyValue2.key);
        assertEquals(BLOCKED, keyValue2.value.getState());
        assertEquals(200.0, keyValue2.value.getAccountBalance());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    @Order(3)
    @DisplayName("Test topology with two messages for each input topic and one ignored message with user state")
    void testTopologyTwoOutputMessagesAndIgnoredUser() {
        //arrange
        MessageUserState messageUserState1 = MessageUserState.builder()
                .userId("abc")
                .state(ACTIVE)
                .timestamp(Instant.now().minusSeconds(10))
                .build();
        MessageUserState messageUserState2 = MessageUserState.builder()
                .userId("abc")
                .state(BLOCKED)
                .timestamp(Instant.now().plusSeconds(8))
                .build();
        MessageUserState messageUserState3 = MessageUserState.builder()
                .userId("xyz")
                .state(ACTIVE)
                .timestamp(Instant.now().plusSeconds(6))
                .build();

        MessageUserBalance messageUserBalance1 = MessageUserBalance.builder()
                .userId("abc")
                .accountBalance(100.0)
                .timestamp(Instant.now().minusSeconds(9))
                .build();

        MessageUserBalance messageUserBalance2 = MessageUserBalance.builder()
                .userId("abc")
                .accountBalance(200.0)
                .timestamp(Instant.now().plusSeconds(7))
                .build();

        //act
        userStateTestInputTopic.pipeInput(messageUserState1.getUserId(), messageUserState1);
        userStateTestInputTopic.pipeInput(messageUserState2.getUserId(), messageUserState2);
        userStateTestInputTopic.pipeInput(messageUserState3.getUserId(), messageUserState3);

        userBalanceTestInputTopic.pipeInput(messageUserBalance1.getUserId(), messageUserBalance1);
        userBalanceTestInputTopic.pipeInput(messageUserBalance2.getUserId(), messageUserBalance2);

        //assert
        KeyValue<String, MessageOutput> keyValue = outputTopic.readKeyValue();
        assertEquals(messageUserState1.getUserId(), keyValue.key);
        assertEquals(BLOCKED, keyValue.value.getState());
        assertEquals(100.0, keyValue.value.getAccountBalance());

        KeyValue<String, MessageOutput> keyValue2 = outputTopic.readKeyValue();
        assertEquals(messageUserState1.getUserId(), keyValue2.key);
        assertEquals(BLOCKED, keyValue2.value.getState());
        assertEquals(200.0, keyValue2.value.getAccountBalance());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("Test topology with two messages for each input topic but different users")
    void testReadFromEmptyTopic() {
        MessageUserState userStateMessage1 = MessageUserState.builder()
                .userId("abc")
                .state(ACTIVE)
                .timestamp(Instant.now().minusSeconds(10))
                .build();
        userStateTestInputTopic.pipeInput(userStateMessage1.getUserId(), userStateMessage1);

        MessageUserBalance userBalanceMessage1 = MessageUserBalance.builder()
                .userId("xyz")
                .accountBalance(100.0)
                .timestamp(Instant.now().minusSeconds(9))
                .build();
        userBalanceTestInputTopic.pipeInput(userBalanceMessage1.getUserId(), userBalanceMessage1);

        MessageUserState userStateMessage2 = MessageUserState.builder()
                .userId("abc")
                .state(BLOCKED)
                .timestamp(Instant.now().minusSeconds(8))
                .build();
        userStateTestInputTopic.pipeInput(userStateMessage2.getUserId(), userStateMessage2);

        MessageUserBalance userBalanceMessage2 = MessageUserBalance.builder()
                .userId("xyz")
                .accountBalance(200.0)
                .timestamp(Instant.now().minusSeconds(7))
                .build();
        userBalanceTestInputTopic.pipeInput(userBalanceMessage2.getUserId(), userBalanceMessage2);

        //Reading from uninitialized topic generate Exception
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(outputTopic::readValue)
                .withMessage("Uninitialized topic: %s", USER_COMBINED_VALUES_TOPIC);
    }
}
