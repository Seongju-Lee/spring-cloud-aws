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

import java.util.HashMap;
import java.util.Map;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sns.endpoints.internal.Value.Str;


public class SnsNotificationRequest {

	private final String subject;
	private final String groupId;
	private final String deduplicationId;

	public SnsNotificationRequest(String subject, String groupId, String deduplicationId) {
		this.subject = subject;
		this.groupId = groupId;
		this.deduplicationId = deduplicationId;
	}

	public String getSubject() {
		return subject;
	}

	public String getGroupId() {
		return groupId;
	}

	public String getDeduplicationId() {
		return deduplicationId;
	}

	public Map<String, Object> getMap() {
		Map<String, Object> headers = new HashMap<>();
		if (subject != null) {
			headers.put(SnsHeaders.NOTIFICATION_SUBJECT_HEADER, subject);
		}
		if (groupId != null) {
			headers.put(SnsHeaders.MESSAGE_GROUP_ID_HEADER, groupId);
		}
		if (deduplicationId != null) {
			headers.put(SnsHeaders.NOTIFICATION_SUBJECT_HEADER, deduplicationId);
		}
		return headers;
	}
}
