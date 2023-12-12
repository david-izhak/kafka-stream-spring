package com.example.kafkastreamspring;

import com.example.kafkastreamspring.config.KafkaConfiguration;
import com.example.kafkastreamspring.config.TopologyConfiguration;
import com.example.kafkastreamspring.model.MessageOutput;
import com.example.kafkastreamspring.model.MessageUserBalance;
import com.example.kafkastreamspring.model.MessageUserState;
import com.example.kafkastreamspring.model.UserState;
import com.example.kafkastreamspring.util.StreamsSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.example.kafkastreamspring.config.TopicNames.*;
import static com.example.kafkastreamspring.model.UserState.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTopology {

    private List<String> output = new ArrayList<>();
    private TestInputTopic<String, MessageUserState> userStateTestInputTopic;
    private TestInputTopic<String, MessageUserBalance> userBalanceTestInputTopic;
    private TestOutputTopic<String, MessageOutput> outputTopic;
    TopologyTestDriver topologyTestDriver;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration(output::add).createTopology(sb);
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
//        userStateTestInputTopic.pipeInput(messageUserState3.getUserId(), messageUserState3);

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
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> {
            outputTopic.readValue();
        }).withMessage("Uninitialized topic: %s", USER_COMBINED_VALUES_TOPIC);
    }
}
