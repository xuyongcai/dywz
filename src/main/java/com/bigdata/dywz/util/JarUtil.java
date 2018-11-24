package com.bigdata.dywz.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * jar包工具类（将程序打成jar包）
 * @author: xiaochai
 * @create: 2018-11-22
 **/
public class JarUtil {
    public static String jar(Class<?> cls){
        String outputJar = cls.getName() + ".jar";
        String input = cls.getClassLoader().getResource("").getFile();
        input = input.substring(0, input.length() - 1);
        input = input.substring(0, input.lastIndexOf("/") + 1);
        input = input + "bin/";

        jar(input, outputJar);
        return outputJar;
    }

    private static void jar(String inputFilename, String outputFileName) {

        JarOutputStream out = null;
        try {
            out = new JarOutputStream(new FileOutputStream(outputFileName));
            File f = new File(inputFilename);
            jar(out, f , "");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void jar(JarOutputStream out, File f, String base) throws IOException {
        if (f.isDirectory()){
            File[] files = f.listFiles();
            base = base.length() == 0 ? "" : base + "/"; //注意，这里用的是左斜杠

            for (int i = 0; i< files.length; i++){
                jar(out, files[i], base + files[i].getName());
            }
        }else {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);

            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1){
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }
}
