/*
 * Copyright (c) 2012-2017 Rafal Lewczuk. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.zorka.tdb.util.lz4;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Original code written by Adrien Grand and Jim Northrup.
// See https://github.com/jpountz/lz4-java and https://gitlab.com/jnorthrup1/lz4-java
//
// Using this copy for general purposes is not recommended.
// Please use original version that is properly maintained and not tailored for ZICO purposes.

/**
 * LZ4 decompressor that requires the size of the original input to be known.
 * <p>
 * Instances of this class are thread-safe.
 */
public abstract class LZ4Decompressor {

    /**
     * Uncompress <code>src[srcOff:]</code> into <code>dest[destOff:destOff+destLen]</code>
     * and return the number of bytes read from <code>src</code>.
     * <code>destLen</code> must be exactly the size of the decompressed data.
     *
     * @param destLen the <b>exact</b> size of the original input
     * @return the number of bytes read to restore the original input
     */
    public abstract int decompress(byte[] src, int srcOff, byte[] dest, int destOff, int destLen);

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}