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

import static io.zorka.tdb.util.lz4.Utils.checkRange;

/**
 * Decompressor written in pure Java without using the unofficial
 * sun.misc.Unsafe API.
 */
public final class LZ4JavaSafeDecompressor extends LZ4Decompressor {

    public static final LZ4Decompressor INSTANCE = new LZ4JavaSafeDecompressor();

    public int decompress(byte[] src, final int srcOff, byte[] dest, final int destOff, int destLen) {
        Utils.checkRange(src, srcOff);
        Utils.checkRange(dest, destOff, destLen);

        if (destLen == 0) {
            if (src[srcOff] != 0) {
                throw new LZ4Exception("Malformed input at " + srcOff);
            }
            return 1;
        }

        final int destEnd = destOff + destLen;

        int sOff = srcOff;
        int dOff = destOff;

        while (true) {
            final int token = src[sOff++] & 0xFF;

            // literals
            int literalLen = token >>> LZ4Utils.ML_BITS;
            if (literalLen == LZ4Utils.RUN_MASK) {
                byte len;
                while ((len = src[sOff++]) == (byte) 0xFF) {
                    literalLen += 0xFF;
                }
                literalLen += len & 0xFF;
            }

            final int literalCopyEnd = dOff + literalLen;
            if (literalCopyEnd > destEnd - LZ4Utils.COPY_LENGTH) {
                if (literalCopyEnd != destEnd) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                } else {
                    LZ4Utils.safeArraycopy(src, sOff, dest, dOff, literalLen);
                    sOff += literalLen;
                    break; // EOF
                }
            }

            LZ4Utils.wildArraycopy(src, sOff, dest, dOff, literalLen);
            sOff += literalLen;
            dOff = literalCopyEnd;

            // matchs
            final int matchDec = (src[sOff++] & 0xFF) | ((src[sOff++] & 0xFF) << 8);
            int matchOff = dOff - matchDec;

            if (matchOff < destOff) {
                throw new LZ4Exception("Malformed input at " + sOff);
            }

            int matchLen = token & LZ4Utils.ML_MASK;
            if (matchLen == LZ4Utils.ML_MASK) {
                byte len;
                while ((len = src[sOff++]) == (byte) 0xFF) {
                    matchLen += 0xFF;
                }
                matchLen += len & 0xFF;
            }
            matchLen += LZ4Utils.MIN_MATCH;

            final int matchCopyEnd = dOff + matchLen;

            if (matchCopyEnd > destEnd - LZ4Utils.COPY_LENGTH) {
                if (matchCopyEnd > destEnd) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                }
                LZ4Utils.safeIncrementalCopy(dest, matchOff, dOff, matchLen);
            } else {
                LZ4Utils.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
            }
            dOff = matchCopyEnd;
        }

        return sOff - srcOff;
    }

}
