package com.example.kafkastreamspring.service;

import com.example.kafkastreamspring.model.MessageUserBalance;
import com.example.kafkastreamspring.model.MessageUserState;
import com.example.kafkastreamspring.model.UserState;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import static com.example.kafkastreamspring.config.StoreNames.USER_BALANCE_STORE;

@Service
@RequiredArgsConstructor
public class UserBalanceService {

    private final KafkaStreams kafkaStreams;

    public Double getLatestUserBalance (String userId) {
        ReadOnlyKeyValueStore<Object, MessageUserBalance> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(USER_BALANCE_STORE,
                        QueryableStoreTypes.keyValueStore()));
        return store.get(userId).getAccountBalance();
    }

}
