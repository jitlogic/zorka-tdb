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

import java.util.Arrays;

import static io.zorka.tdb.util.lz4.Utils.checkRange;

/**
 * Fast compressor written in pure Java without using the unofficial
 * sun.misc.Unsafe API.
 */
public final class LZ4JavaSafeCompressor extends LZ4Compressor {

    public static final LZ4Compressor INSTANCE = new LZ4JavaSafeCompressor();

    private int compress64k(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int destEnd) {
        final int srcEnd = srcOff + srcLen;
        final int srcLimit = srcEnd - LZ4Utils.LAST_LITERALS;
        final int mflimit = srcEnd - LZ4Utils.MF_LIMIT;

        int sOff = srcOff, dOff = destOff;

        int anchor = sOff;

        if (srcLen > LZ4Utils.MIN_LENGTH) {

            final short[] hashTable = new short[LZ4Utils.HASH_TABLE_SIZE_64K];

            ++sOff;

            main:
            while (true) {

                // find a match
                int forwardOff = sOff;

                int ref;
                int findMatchAttempts = (1 << LZ4Utils.SKIP_STRENGTH) + 3;
                do {
                    sOff = forwardOff;
                    forwardOff += findMatchAttempts++ >>> LZ4Utils.SKIP_STRENGTH;

                    if (forwardOff > mflimit) {
                        break main;
                    }

                    final int h = LZ4Utils.hash64k(src, sOff);
                    ref = srcOff + (hashTable[h] & 0xFFFF);
                    hashTable[h] = (short) (sOff - srcOff);
                } while (!LZ4Utils.readIntEquals(src, ref, sOff));

                // catch up
                final int excess = LZ4Utils.commonBytesBackward(src, ref, sOff, srcOff, anchor);
                sOff -= excess;
                ref -= excess;

                // sequence == refsequence
                final int runLen = sOff - anchor;

                // encode literal length
                int tokenOff = dOff++;

                if (dOff + runLen + (2 + 1 + LZ4Utils.LAST_LITERALS) + (runLen >>> 8) > destEnd) {
                    throw new LZ4Exception("maxDestLen is too small");
                }

                int token;
                if (runLen >= LZ4Utils.RUN_MASK) {
                    token = LZ4Utils.RUN_MASK << LZ4Utils.ML_BITS;
                    dOff = LZ4Utils.writeLen(runLen - LZ4Utils.RUN_MASK, dest, dOff);
                } else {
                    token = runLen << LZ4Utils.ML_BITS;
                }

                // copy literals
                LZ4Utils.wildArraycopy(src, anchor, dest, dOff, runLen);
                dOff += runLen;

                while (true) {
                    // encode offset
                    final int back = sOff - ref;
                    dest[dOff++] = (byte) back;
                    dest[dOff++] = (byte) (back >>> 8);

                    // count nb matches
                    sOff += LZ4Utils.MIN_MATCH;
                    final int matchLen = LZ4Utils.commonBytes(src, ref + LZ4Utils.MIN_MATCH, sOff, srcLimit);
                    if (dOff + (1 + LZ4Utils.LAST_LITERALS) + (matchLen >>> 8) > destEnd) {
                        throw new LZ4Exception("maxDestLen is too small");
                    }
                    sOff += matchLen;

                    // encode match len
                    if (matchLen >= LZ4Utils.ML_MASK) {
                        token |= LZ4Utils.ML_MASK;
                        dOff = LZ4Utils.writeLen(matchLen - LZ4Utils.ML_MASK, dest, dOff);
                    } else {
                        token |= matchLen;
                    }
                    dest[tokenOff] = (byte) token;

                    // test end of chunk
                    if (sOff > mflimit) {
                        anchor = sOff;
                        break main;
                    }

                    // fill table
                    hashTable[LZ4Utils.hash64k(src, sOff - 2)] = (short) (sOff - 2 - srcOff);

                    // test next position
                    final int h = LZ4Utils.hash64k(src, sOff);
                    ref = srcOff + (hashTable[h] & 0xFFFF);
                    hashTable[h] = (short) (sOff - srcOff);

                    if (!LZ4Utils.readIntEquals(src, sOff, ref)) {
                        break;
                    }

                    tokenOff = dOff++;
                    token = 0;
                }

                // prepare next loop
                anchor = sOff++;
            }
        }

        dOff = LZ4Utils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
        return dOff - destOff;
    }

    @Override
    public final int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
                              int destOff, int maxDestLen) {
        Utils.checkRange(src, srcOff, srcLen);
        Utils.checkRange(dest, destOff, maxDestLen);
        final int destEnd = destOff + maxDestLen;

        if (srcLen < LZ4Utils.LZ4_64K_LIMIT) {
            return compress64k(src, srcOff, srcLen, dest, destOff, destEnd);
        }

        final int srcEnd = srcOff + srcLen;
        final int srcLimit = srcEnd - LZ4Utils.LAST_LITERALS;
        final int mflimit = srcEnd - LZ4Utils.MF_LIMIT;

        int sOff = srcOff, dOff = destOff;
        int anchor = sOff++;

        final int[] hashTable = new int[LZ4Utils.HASH_TABLE_SIZE];
        Arrays.fill(hashTable, anchor);

        main:
        while (true) {

            // find a match
            int forwardOff = sOff;

            int ref;
            int findMatchAttempts = (1 << LZ4Utils.SKIP_STRENGTH) + 3;
            int back;
            do {
                sOff = forwardOff;
                forwardOff += findMatchAttempts++ >>> LZ4Utils.SKIP_STRENGTH;

                if (forwardOff > mflimit) {
                    break main;
                }

                final int h = LZ4Utils.hash(src, sOff);
                ref = hashTable[h];
                back = sOff - ref;
                hashTable[h] = sOff;
            } while (back >= LZ4Utils.MAX_DISTANCE || !LZ4Utils.readIntEquals(src, ref, sOff));

            final int excess = LZ4Utils.commonBytesBackward(src, ref, sOff, srcOff, anchor);
            sOff -= excess;
            ref -= excess;

            // sequence == refsequence
            final int runLen = sOff - anchor;

            // encode literal length
            int tokenOff = dOff++;

            if (dOff + runLen + (2 + 1 + LZ4Utils.LAST_LITERALS) + (runLen >>> 8) > destEnd) {
                throw new LZ4Exception("maxDestLen is too small");
            }

            int token;
            if (runLen >= LZ4Utils.RUN_MASK) {
                token = LZ4Utils.RUN_MASK << LZ4Utils.ML_BITS;
                dOff = LZ4Utils.writeLen(runLen - LZ4Utils.RUN_MASK, dest, dOff);
            } else {
                token = runLen << LZ4Utils.ML_BITS;
            }

            // copy literals
            LZ4Utils.wildArraycopy(src, anchor, dest, dOff, runLen);
            dOff += runLen;

            while (true) {
                // encode offset
                dest[dOff++] = (byte) back;
                dest[dOff++] = (byte) (back >>> 8);

                // count nb matches
                sOff += LZ4Utils.MIN_MATCH;
                final int matchLen = LZ4Utils.commonBytes(src, ref + LZ4Utils.MIN_MATCH, sOff, srcLimit);
                if (dOff + (1 + LZ4Utils.LAST_LITERALS) + (matchLen >>> 8) > destEnd) {
                    throw new LZ4Exception("maxDestLen is too small");
                }
                sOff += matchLen;

                // encode match len
                if (matchLen >= LZ4Utils.ML_MASK) {
                    token |= LZ4Utils.ML_MASK;
                    dOff = LZ4Utils.writeLen(matchLen - LZ4Utils.ML_MASK, dest, dOff);
                } else {
                    token |= matchLen;
                }
                dest[tokenOff] = (byte) token;

                // test end of chunk
                if (sOff > mflimit) {
                    anchor = sOff;
                    break main;
                }

                // fill table
                hashTable[LZ4Utils.hash(src, sOff - 2)] = sOff - 2;

                // test next position
                final int h = LZ4Utils.hash(src, sOff);
                ref = hashTable[h];
                hashTable[h] = sOff;
                back = sOff - ref;

                if (back >= LZ4Utils.MAX_DISTANCE || !LZ4Utils.readIntEquals(src, ref, sOff)) {
                    break;
                }

                tokenOff = dOff++;
                token = 0;
            }

            // prepare next loop
            anchor = sOff++;
        }

        dOff = LZ4Utils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
        return dOff - destOff;
    }

}
