package com.example.kafkastreamspring.config;

import com.example.kafkastreamspring.model.MessageOutput;
import com.example.kafkastreamspring.model.MessageUserBalance;
import com.example.kafkastreamspring.model.MessageUserState;
import com.example.kafkastreamspring.util.StreamsSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.example.kafkastreamspring.config.StoreNames.USER_BALANCE_STORE;
import static com.example.kafkastreamspring.config.StoreNames.USER_STATE_STORE;
import static com.example.kafkastreamspring.config.TopicNames.*;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class TopologyConfiguration {


    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, MessageUserState> userStateInput = streamsBuilder.
                stream(USER_STATE_TOPIC,
                        Consumed.with(Serdes.String(),
                                StreamsSerdes.messageUserStateSerde()
                        )
                );

        KTable<String, MessageUserState> userStateKTable = userStateInput.groupByKey()
                .reduce((aggValue, newValue) -> {
                            if (aggValue.getTimestamp().isBefore(newValue.getTimestamp())) {
                                return newValue;
                            } else {
                                return aggValue;
                            }
                        },
                        Materialized.as(USER_STATE_STORE)
//                        Materialized.with(Serdes.String(), StreamsSerdes.messageUserStateSerde())
                );

        KStream<String, MessageUserBalance> eventScores = streamsBuilder
                .stream(USER_BALANCE_TOPIC,
                        Consumed.with(Serdes.String(),
                                StreamsSerdes.messageUserBalanceSerde()
                        )
                );

        KTable<String, MessageUserBalance> userBalanceKTable = eventScores
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                            if (aggValue.getTimestamp().isBefore(newValue.getTimestamp())) {
                                return newValue;
                            } else {
                                return aggValue;
                            }
                        },
                        Materialized.as(USER_BALANCE_STORE)
//                        Materialized.with(Serdes.String(), StreamsSerdes.messageUserBalanceSerde())
                );

        KTable<String, MessageOutput> joinedMessageOutput = userStateKTable.join(userBalanceKTable,
                (userState, userBalance) -> MessageOutput.builder()
                        .userId(userState.getUserId())
                        .state(userState.getState())
                        .accountBalance(userBalance.getAccountBalance())
                        .timestamp(
                                userState.getTimestamp().isBefore(userBalance.getTimestamp())
                                        ? userBalance.getTimestamp() : userState.getTimestamp())
                        .build());

        joinedMessageOutput.toStream().peek((k, v) -> log.info(v.toString())).to(USER_COMBINED_VALUES_TOPIC, Produced.with(Serdes.String(),
                StreamsSerdes.messageOutputSerde()));

        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");
        return topology;
    }
}
