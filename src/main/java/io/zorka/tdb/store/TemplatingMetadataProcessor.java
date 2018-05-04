package io.zorka.tdb.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.util.ObjectInspector;
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.util.ObjectInspector;
import io.zorka.tdb.util.ZicoUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class TemplatingMetadataProcessor implements ChunkMetadataProcessor {

    private Map<Integer,String> templates = new HashMap<>();

    private String substitute(String input, Map<Object,Object> attrs, StructuredTextIndex sti) {
        Matcher m = ObjectInspector.reVarSubstPattern.matcher(input);
        StringBuffer sb = new StringBuffer();

        while (m.find()) {
            String expr = m.group(1), def = null;
            if (expr.contains(":")) {
                String[] s = expr.split(":");
                expr = s[0];
                def = s[1];
            }
            Integer len = null;
            if (expr.contains("~")) {
                String[] s = expr.split("~");
                expr = s[0];
                len = Integer.parseInt(s[1]);
            }
            Object val = null;
            for (String exp : expr.split("\\|")) {
                String[] segs = exp.split("\\.");
                val = attrs.get(segs[0]);
                if (val == null) {
                    int id = sti.get(segs[0]);
                    if (id != -1) val = attrs.get(new ObjectRef(id));
                }
                for (int i = 1; i < segs.length; i++) {
                    val = ObjectInspector.getAttr(val, segs[i]);
                    if (val == null) {
                        int id = sti.get(segs[i]);
                        if (id != -1) val = attrs.get(new ObjectRef(id));
                    }
                    // TODO what about recursive maps and refs ?
                }
                if (val instanceof ObjectRef) {
                    val = sti.gets(((ObjectRef)val).id);
                    // TODO this will be more nuanced with recursive maps, prepare some unit tests for recursive maps
                }
                if (val != null) break;
            }
            val = val != null ? val : def;
            String s = ZicoUtil.castString(val);
            if (len != null && s.length() > len) {
                s = s.substring(0, len);
            }
            m.appendReplacement(sb, ObjectInspector.reDollarSign.matcher(s).replaceAll(ObjectInspector.reDollarReplacement));
        }

        m.appendTail(sb);

        return sb.toString();

    }

    public void setTemplates(Map<Integer,String> templates) {
        this.templates = templates;
    }

    public Map<Integer,String> getTemplates() {
        return templates;
    }

    public void putTemplate(int typeId, String template) {
        templates.put(typeId, template);
    }

    public void delTemplate(int typeId) {
        templates.remove(typeId);
    }

    public String getTemplate(int typeId) {
        return templates.get(typeId);
    }

    public void clearTemplates() {
        templates.clear();
    }

    @Override
    public void process(ChunkMetadata md, SimpleTraceStore store) {
        if (md.getAttrs() != null) {
            String tmpl = templates.get(md.getTypeId());
            if (tmpl != null) {
                md.setDescId(store.getTextIndex().add(substitute(tmpl, md.getAttrs(), store.getTextIndex())));
            }
        }
    }
}
