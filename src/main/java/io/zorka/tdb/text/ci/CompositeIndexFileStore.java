/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
 * <p/>
 * This is free software. You can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p/>
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this software. If not, see <http://www.gnu.org/licenses/>.
 */

package io.zorka.tdb.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.text.fm.BWT;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.WalTextIndex;
import io.zorka.tdb.text.WritableTextIndex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompositeIndexFileStore implements CompositeIndexStore {
    private final static Pattern RE_WAL = Pattern.compile("^(\\w+)-([0-9a-f]{8})\\.wal$");
    private final static Pattern RE_FMI = Pattern.compile("^(\\w+)-([0-9a-f]{8})-([0-9a-f]{8})\\.fmi$");

    private File path;
    private String name;
    private int baseSize;


    public CompositeIndexFileStore(String path, String name, Properties props) {
        this.path = new File(path);
        this.name = name;
        this.baseSize = Integer.parseInt(props.getProperty("rotation.base_size", CompositeIndex.DEFAULT_WAL_SIZE+"")) * 1024;
    }


    @Override
    public String getPath() {
        return path.getPath();
    }


    @Override
    public List<TextIndex> listAll() {
        String[] fnames = path.list();
        if (fnames == null) throw new ZicoException("Directory does not exist: " + path);
        List<TextIndex> rslt = new ArrayList<>();
        for (String fname : fnames) {
            Matcher mw = RE_WAL.matcher(fname);
            if (mw.matches() && mw.group(1).equals(name)) {
                File file = new File(path, fname);
                int idBase = Integer.parseInt(mw.group(2), 16);
                rslt.add(new WalTextIndex(file, idBase, baseSize));
            }
            Matcher mf = RE_FMI.matcher(fname);
            if (mf.matches() && mf.group(1).equals(name)) {
                File file = new File(path, fname);
                rslt.add(new FmTextIndex(file));
            }
        }
        return rslt;
    }


    @Override
    public WritableTextIndex addIndex(int idBase) {
        String fname = String.format("%s-%08x.wal", name, idBase);
        File f = new File(path, fname);
        if (f.exists()) throw new ZicoException("File already exists: " + f);
        return new WalTextIndex(f, idBase, baseSize);
    }


    @Override
    public void removeIndex(TextIndex index) {
        File f = new File(index.getPath());
        try {
            index.close();
            if (f.exists()) {
                if (!f.delete())
                    // TODO or maybe only log problem
                    throw new IOException("Cannot delete file: " + f);
            }
        } catch (IOException e) {
            e.printStackTrace(); // TODO log this
        }
    }


    @Override
    public TextIndex compressIndex(TextIndex index) {
        int idBase = index.getIdBase(), nWords = index.getNWords();
        File idxf = new File(path, String.format("%s-%08x-%08x.fmi", name, idBase, nWords));
        if (idxf.exists()) {
            if (!idxf.delete()) {
                System.out.println("Cannot delete file: " + idxf);
            }
        }
        try {
            byte[] data = ((WalTextIndex)index).getData(true, true, true);
            FmIndexFileStoreBuilder fib = new FmIndexFileStoreBuilder(idxf, FmCompressionLevel.DEFAULT);
            fib.rawToFm(data, index.getNWords(), idBase);
            fib.close();
        } catch (IOException e) {
            throw new ZicoException("Cannot compress index", e);
        }


        return new FmTextIndex(idxf);
    }


    @Override
    public TextIndex mergeIndex(List<TextIndex> indexes) {

        byte[] data = new byte[(int) indexes.stream().mapToLong(TextIndex::getDatalen).sum()];
        int dpos = 0, idBase = Integer.MAX_VALUE, nWords = 0;

        for (TextIndex index : indexes) {
            FmTextIndex fx = (FmTextIndex) index;
            byte[] buf = new byte[(int)fx.getDatalen()];
            if (fx.getFileStore().getData(buf, 0) != buf.length) {
                throw new ZicoException("Cannot retrieve raw BWT data from index " + index);
            }
            buf = BWT.bwtdecode(buf, fx.getPidx());
            System.arraycopy(buf, 0, data, dpos, buf.length);
            dpos += buf.length;
            idBase = Math.min(idBase, fx.getIdBase());
            nWords += fx.getNWords();
        }

        File idxf = new File(path, String.format("%s-%08x-%08x.fmi", name, idBase, nWords));

        if (idxf.exists()) {
            if (!idxf.delete()) {
                System.out.println("Cannot delete file: " + idxf);
            }
        }

        try {
            FmIndexFileStoreBuilder fib = new FmIndexFileStoreBuilder(idxf, FmCompressionLevel.DEFAULT);
            fib.rawToFm(data, nWords, idBase);
            fib.close();
        } catch (IOException e) {
            throw new ZicoException("Cannot compress FM index", e);
        }

        return new FmTextIndex(idxf);
    }
}
