package com.example.kafkastreamspring.service;

import com.example.kafkastreamspring.model.MessageUserState;
import com.example.kafkastreamspring.model.UserState;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import static com.example.kafkastreamspring.config.StoreNames.USER_STATE_STORE;

@Service
@RequiredArgsConstructor
public class UserStateService {

    private final KafkaStreams kafkaStreams;

    public UserState getLatestUserState(String userId) {
        ReadOnlyKeyValueStore<Object, MessageUserState> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(USER_STATE_STORE,
                        QueryableStoreTypes.keyValueStore()));
        return store.get(userId).getState();
    }
}
