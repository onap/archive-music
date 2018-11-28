/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.lockingservice;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

/**
 * Represents an ephemeral znode name which has an ordered sequence number and can be sorted in
 * order
 *
 */
class ZNodeName implements Comparable<ZNodeName> {
    private final String name;
    private String prefix;
    private int sequence = -1;
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(ZNodeName.class);
    
    public ZNodeName(String name) {
        if (name == null) {
            throw new NullPointerException("id cannot be null");
        }
        this.name = name;
        this.prefix = name;
        int idx = name.lastIndexOf('-');
        if (idx >= 0) {
            this.prefix = name.substring(0, idx);
            try {
                this.sequence = Integer.parseInt(name.substring(idx + 1));
                // If an exception occurred we misdetected a sequence suffix,
                // so return -1.
            } catch (NumberFormatException e) {
                logger.error("Exception", e);
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),"Number format exception "+idx, ErrorSeverity.ERROR, ErrorTypes.GENERALSERVICEERROR);
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error("Exception", e);
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),"Array out of bounds for  "+idx, ErrorSeverity.ERROR, ErrorTypes.GENERALSERVICEERROR);
            }
        }
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ZNodeName sequence = (ZNodeName) o;

        if (!name.equals(sequence.name))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + 37;
    }

    public int compareTo(ZNodeName that) {
        int answer = this.prefix.compareTo(that.prefix);
        if (answer == 0) {
            int s1 = this.sequence;
            int s2 = that.sequence;
            if (s1 == -1 && s2 == -1) {
                return this.name.compareTo(that.name);
            }
            answer = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
        }
        return answer;
    }

    /**
     * Returns the name of the znode
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the sequence number
     */
    public int getZNodeName() {
        return sequence;
    }

    /**
     * Returns the text prefix before the sequence number
     */
    public String getPrefix() {
        return prefix;
    }
}
