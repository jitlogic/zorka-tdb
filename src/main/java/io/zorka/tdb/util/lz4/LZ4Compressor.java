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
 * LZ4 compressor.
 * <p>
 * Instances of this class are thread-safe.
 */
public abstract class LZ4Compressor {

    /**
     * Return the maximum compressed length for an input of size <code>length</code>.
     */
    public final int maxCompressedLength(int length) {
        return LZ4Utils.maxCompressedLength(length);
    }

    /**
     * Compress <code>src[srcOff:srcOff+srcLen]</code> into
     * <code>dest[destOff:destOff+destLen]</code> and return the compressed
     * length.
     * <p>
     * This method will throw a {@link LZ4Exception} if this compressor is unable
     * to compress the input into less than <code>maxDestLen</code> bytes. To
     * prevent this exception to be thrown, you should make sure that
     * <code>maxDestLen >= maxCompressedLength(srcLen)</code>.
     *
     * @return the compressed size
     * @throws LZ4Exception if maxDestLen is too small
     */
    public abstract int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

    /**
     * Convenience method. Equivalent to calling
     * {@link #compress(byte[], int, int, byte[], int, int)} with
     * <code>destLen = dest.length - destOff</code>.
     */
    public final int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff) {
        return compress(src, srcOff, srcLen, dest, destOff, dest.length - destOff);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
