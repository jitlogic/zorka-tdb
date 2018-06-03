/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.test.support;

import org.objectweb.asm.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Stack;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

/**
 *
 */
public class JarScanStrGen implements TestStrGen {

    private Stack<File> fstack = new Stack<>();
    private File root;
    private Pattern RE_JAR = Pattern.compile(".*\\.jar");
    private Pattern RE_SRC = Pattern.compile(".*-sources\\.jar");
    private Pattern RE_DOC = Pattern.compile(".*-javadoc\\.jar");

    private File curFile;
    private JarFile jarFile;
    private Enumeration<JarEntry> jarEnum;

    private Stack<String> sstack = new Stack<>();

    private int numFiles = 0, numClasses = 0;
    private int readErrors = 0, procErrors = 0, findErrors = 0, zipErrors = 0;

    public JarScanStrGen() throws IOException {
        File home = new File(System.getProperty("user.home"));
        root = new File(home, ".m2");
        fstack.push(root);
    }

    public JarScanStrGen(String path) throws IOException {
        root = new File(path);
        fstack.push(root);
    }


    private boolean nextFile() throws IOException {

        if (jarFile != null) {
            jarFile.close();
            jarFile = null;
            jarEnum = null;
        }

        while (!fstack.isEmpty() && !fstack.peek().isFile()) {
            File d = fstack.pop();
            if (d.isDirectory()) {
                String[] names = d.list();
                if (names != null) {
                    Arrays.sort(names);
                    for (String n : names) {
                        File f = new File(d, n);
                        if (f.isDirectory() || (RE_JAR.matcher(f.getName()).matches()
                                && !RE_SRC.matcher(f.getName()).matches())
                                && !RE_DOC.matcher(f.getName()).matches()) {
                            fstack.push(f);
                        }
                    }
                } else {
                    System.out.println("Error listing directory: " + d);
                    findErrors++;
                }
            }
        }

        File f = fstack.isEmpty() ? null : fstack.pop();

        if (f != null) {
            curFile = f;
            numFiles++;
            //System.out.println("File: " + curFile);
            try {
                jarFile = new JarFile(curFile);
                jarEnum = jarFile.entries();
            } catch (ZipException e) {
                jarFile = null;
                jarEnum = null;
                System.err.println("Skipping file: " + curFile + " due to error: " + e);
                zipErrors++;
                return nextFile();
            }
            return true;
        } else {
            return false;
        }
    }

    private void processEntry(JarEntry je) {
        numClasses++;
        try {
            int size = (int) je.getSize();
            if (size > 0) {
                byte[] classbytes = new byte[size];
                try (InputStream is = jarFile.getInputStream(je)) {
                    int pos = 0, len = classbytes.length;
                    while (pos < len) {
                        int sz = is.read(classbytes, pos, len-pos);
                        if (sz <= 0) {
                            System.err.println("Error reading entry " + je + " from " + jarFile);
                            readErrors++;
                            break;
                        }
                        pos += sz;
                    }
                } catch (IOException e) {
                    System.err.println("I/O Error reading entry " + je + " from " + jarFile + ": " + e);
                    readErrors++;
                }
                ClassReader cr = new ClassReader(classbytes);
                ClassVisitor cv = new ListingClassVisitor();
                cr.accept(cv, 0);
            }
        } catch (Exception e) {
            System.err.println("Error processing entry " + je + " from " + jarFile + ": " + e);
            procErrors++;
        }
    }


    public boolean nextEntry() throws IOException {
        while (jarEnum == null || !jarEnum.hasMoreElements()) {
            if (!nextFile()) return false;
        }

        do {
            if (!jarEnum.hasMoreElements()) {
                return true;
            }
            JarEntry je = jarEnum.nextElement();
            if (je != null && je.getName().endsWith(".class")) {
                String name = je.getName();
                sstack.push(name.substring(0, name.length()-6));
                processEntry(je);
            }
        } while (sstack.isEmpty());

        return true;
    }

    @Override
    public String get() {
        try {
            while (sstack.isEmpty()) {
                if (!nextEntry()) return null;
            }
            return sstack.pop();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void reset() {
        sstack.clear();
        fstack.clear();
            if (jarFile != null) {
                try {
                    jarFile.close();
                } catch (IOException e) {
                    // ignore
                }
                jarFile = null;
                jarEnum = null;
                curFile = null;
                numFiles = numClasses = findErrors = readErrors = procErrors = zipErrors = 0;
            }
    }

    private void push(String s) {
        if (s != null) {
            sstack.push(s);
        }
    }


    public void printStats() {
        System.out.println("Files processed: " + numFiles);
        System.out.println("Classes processed: " + numClasses);
        System.out.println("Read errors: " + readErrors);
        System.out.println("Zip errors: " + zipErrors);
        System.out.println("Parse errors: " + procErrors);
    }

    private class ListingAnnotationVisitor extends AnnotationVisitor {

        public ListingAnnotationVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public void visit(String name, Object value) {
            push(name);
            if (value instanceof String) {
                push((String)value);
            }
        }

        @Override
        public void visitEnum(String name, String desc, String value) {
            push(name);
            push(desc);
            push(value);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String name, String desc) {
            push(name);
            push(desc);
            return this;
        }

        @Override
        public AnnotationVisitor visitArray(String name) {
            push(name);
            return this;
        }
    }


    private class ListingFieldVisitor extends FieldVisitor {

        private AnnotationVisitor av;

        public ListingFieldVisitor(AnnotationVisitor av) {
            super(Opcodes.ASM5);
            this.av = av;
        }

        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            push(desc);
            return av;
        }

        public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
            if (typePath != null) push(typePath.toString());
            push(desc);
            return av;
        }

        public void visitAttribute(Attribute attr) {
            if (attr != null && attr.type != null) {
                push(attr.type);
            }
        }

    }


    private class ListingMethodVisitor extends MethodVisitor {

        private AnnotationVisitor av;

        public ListingMethodVisitor(AnnotationVisitor av) {
            super(Opcodes.ASM5);
            this.av = av;
        }

        public void visitParameter(String name, int access) {
            push(name);
        }

        public AnnotationVisitor visitAnnotationDefault() {
            return av;
        }

        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            push(desc);
            return av;
        }

        public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
            if (typePath != null) push(typePath.toString());
            push(desc);
            return av;
        }

        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
            push(desc);
            return av;
        }

        public void visitAttribute(Attribute attr) {
            if (attr != null && attr.type != null) push(attr.type);
        }

        public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
            if (local != null) {
                for (Object l : local) {
                    if (l instanceof String) {
                        push((String)l);
                    }
                }
            }

            if (stack != null) {
                for (Object s : stack) {
                    if (s instanceof String) {
                        push((String)s);
                    }
                }
            }
        }

        public void visitTypeInsn(int opcode, String type) {
            push(type);
        }

        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
            push(owner);
            push(name);
            push(desc);
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            push(owner);
            push(name);
            push(desc);
        }

        public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
            push(name);
            push(desc);
            if (bsm != null) {
                push(bsm.getName());
                push(bsm.getDesc());
                push(bsm.getOwner());
            }

            if (bsmArgs != null) {
                for (Object a : bsmArgs) {
                    if (a instanceof String) {
                        push((String)a);
                    }
                }
            }
        }

        public void visitLdcInsn(Object cst) {
            if (cst instanceof String) push((String)cst);
        }

        public void visitMultiANewArrayInsn(String desc, int dims) {
            push(desc);
        }

        public AnnotationVisitor visitInsnAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
            push(desc);
            return av;
        }

        public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
            push(type);
        }

        public AnnotationVisitor visitTryCatchAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
            push(desc);
            return av;
        }

        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
            push(name);
            push(desc);
            push(signature);
        }

        public AnnotationVisitor visitLocalVariableAnnotation(int typeRef, TypePath typePath, Label[] start,
                                                              Label[] end, int[] index, String desc, boolean visible) {
            push(desc);
            return av;
        }


    }


    private class ListingClassVisitor extends ClassVisitor {

        private AnnotationVisitor av = new ListingAnnotationVisitor();
        private FieldVisitor fv = new ListingFieldVisitor(av);
        private MethodVisitor mv = new ListingMethodVisitor(av);

        ListingClassVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            push(name);
            push(signature);
            push(superName);
            if (interfaces != null) {
                for (String ifc : interfaces) {
                    push(ifc);
                }
            }
        }

        @Override
        public void visitSource(String source, String debug) {
            push(source);
            push(debug);
        }

        @Override
        public void visitOuterClass(String owner, String name, String desc) {
            push(owner);
            push(name);
            push(desc);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            push(desc);
            return av;
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
            if (typePath != null) push(typePath.toString());
            push(desc);
            return av;
        }

        @Override
        public void visitAttribute(Attribute attr) {
            push(attr.type);
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {
            push(name);
            push(outerName);
            push(innerName);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            push(name);
            push(desc);
            push(signature);
            if (value instanceof String) push((String)value);
            return fv;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            push(name);
            push(desc);
            push(signature);
            if (exceptions != null) {
                for (String e : exceptions) {
                    push(e);
                }
            }
            return mv;
        }
    }
}
