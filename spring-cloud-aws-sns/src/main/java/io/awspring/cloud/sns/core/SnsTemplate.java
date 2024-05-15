/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sns.core;

import static io.awspring.cloud.sns.core.SnsHeaders.*;
import static io.awspring.cloud.sns.core.SnsHeaders.NOTIFICATION_SUBJECT_HEADER;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.core.AbstractMessageSendingTemplate;
import org.springframework.messaging.core.DestinationResolvingMessageSendingOperations;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.util.Assert;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsClient;

/**
 * Helper class that simplifies synchronous sending of notifications to SNS. The only mandatory fields are
 * {@link SnsClient} and AutoCreate boolean.
 *
 * @author Alain Sahli
 * @author Matej Nedic
 * @author Mariusz Sondecki
 * @since 1.0
 */
public class SnsTemplate extends AbstractMessageSendingTemplate<TopicMessageChannel>
		implements DestinationResolvingMessageSendingOperations<TopicMessageChannel>, SnsOperations {

	private final SnsClient snsClient;
	private final TopicArnResolver topicArnResolver;
	private final List<ChannelInterceptor> channelInterceptors = new ArrayList<>();

	public SnsTemplate(SnsClient snsClient) {
		this(snsClient, null);
	}

	public SnsTemplate(SnsClient snsClient, @Nullable MessageConverter messageConverter) {
		this(snsClient, new CachingTopicArnResolver(new DefaultTopicArnResolver(snsClient)), messageConverter);
	}

	public SnsTemplate(SnsClient snsClient, TopicArnResolver topicArnResolver,
			@Nullable MessageConverter messageConverter) {
		Assert.notNull(snsClient, "SnsClient must not be null");
		Assert.notNull(topicArnResolver, "topicArnResolver must not be null");
		this.topicArnResolver = topicArnResolver;
		this.snsClient = snsClient;

		if (messageConverter != null) {
			this.setMessageConverter(initMessageConverter(messageConverter));
		}
	}

	public void setDefaultDestinationName(@Nullable String defaultDestination) {
		super.setDefaultDestination(
				defaultDestination == null ? null : resolveMessageChannelByTopicName(defaultDestination));
	}

	@Override
	protected void doSend(TopicMessageChannel destination, Message<?> message) {
		destination.send(message);
	}

	@Override
	public void send(String destination, Message<?> message) throws MessagingException {
		doSend(resolveMessageChannelByTopicName(destination), message);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload) throws MessagingException {
		this.convertAndSend(destination, payload, null, null);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable Map<String, Object> headers)
			throws MessagingException {
		this.convertAndSend(destination, payload, headers, null);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable MessagePostProcessor postProcessor)
			throws MessagingException {
		this.convertAndSend(destination, payload, null, postProcessor);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable Map<String, Object> headers,
			@Nullable MessagePostProcessor postProcessor) throws MessagingException {
		convertAndSend(resolveMessageChannelByTopicName(destination), payload, headers, postProcessor);
	}

	/**
	 * Convenience method that sends a notification with the given {@literal message} and {@literal subject} to the
	 * {@literal destination}. The {@literal subject} is sent as header as defined in the
	 * <a href="https://docs.aws.amazon.com/sns/latest/dg/json-formats.html">SNS message JSON formats</a>.
	 *
	 * @param destinationName The logical name of the destination
	 * @param message The message to send
	 * @param subject The subject to send
	 */
	public void sendNotification(String destinationName, Object message, @Nullable String subject) {
		this.convertAndSend(destinationName, message, Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	public void sendNotification(String destinationName, Object message, @Nullable String subject, @Nullable String groupId, @Nullable String deduplicationId) {
		final Map<String, Object> headers = new HashMap<>();
		if (subject != null) headers.put("notification-subject", subject);
		if (groupId != null) headers.put("message-group-id", groupId);
		if (deduplicationId != null) headers.put("message-deduplication-id", deduplicationId);

		final Map<String, Object> immutableHeaders = headers.entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
		this.convertAndSend(destinationName, message, immutableHeaders);
	}

	/**
	 * Convenience method that sends a notification with the given {@literal message} and {@literal subject} to the
	 * {@literal destination}. The {@literal subject} is sent as header as defined in the
	 * <a href="https://docs.aws.amazon.com/sns/latest/dg/json-formats.html">SNS message JSON formats</a>. The
	 * configured default destination will be used.
	 *
	 * @param message The message to send
	 * @param subject The subject to send
	 */
	public void sendNotification(Object message, @Nullable String subject) {
		this.convertAndSend(getRequiredDefaultDestination(), message,
				Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	/**
	 * Add a {@link ChannelInterceptor} to be used by {@link TopicMessageChannel} created with this template.
	 * Interceptors will be applied just after TopicMessageChannel creation.
	 *
	 * @param channelInterceptor the message interceptor instance.
	 */
	public void addChannelInterceptor(ChannelInterceptor channelInterceptor) {
		Assert.notNull(channelInterceptor, "channelInterceptor cannot be null");
		this.channelInterceptors.add(channelInterceptor);
	}

	@Override
	public void sendNotification(String topic, SnsNotification<?> notification) {
		this.convertAndSend(topic, notification.getPayload(), notification.getHeaders());
	}

	private TopicMessageChannel resolveMessageChannelByTopicName(String topicName) {
		Arn topicArn = this.topicArnResolver.resolveTopicArn(topicName);
		TopicMessageChannel topicMessageChannel = new TopicMessageChannel(this.snsClient, topicArn);
		channelInterceptors.forEach(topicMessageChannel::addInterceptor);
		return topicMessageChannel;
	}

	private static CompositeMessageConverter initMessageConverter(@Nullable MessageConverter messageConverter) {
		List<MessageConverter> converters = new ArrayList<>();

		StringMessageConverter stringMessageConverter = new StringMessageConverter();
		stringMessageConverter.setSerializedPayloadClass(String.class);
		converters.add(stringMessageConverter);

		if (messageConverter != null) {
			converters.add(messageConverter);
		}

		return new CompositeMessageConverter(converters);
	}


	private SnsNotificationRequest extractSnsNotificationRequest(Object message) {
		Class<?> clazz = message.getClass(); // 객체의 클래스 정보를 얻음
		while (clazz != null) { // 상위 클래스까지 포함하여 검사
			Field[] fields = clazz.getDeclaredFields(); // 해당 클래스의 모든 필드를 얻음
			for (Field field : fields) {
				if (SnsNotificationRequest.class.isAssignableFrom(field.getType())) {
					field.setAccessible(true); // private 필드 접근을 위해 접근 가능하도록 설정
					try {
						return (SnsNotificationRequest) field.get(message); // 필드의 값을 가져옴
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} finally {
						field.setAccessible(false); // 접근성 설정을 원래대로 되돌림
					}
				}
			}
			clazz = clazz.getSuperclass(); // 상위 클래스로 이동
		}
		return null; // 해당 타입의 필드가 없으면 null 반환
	}
}
