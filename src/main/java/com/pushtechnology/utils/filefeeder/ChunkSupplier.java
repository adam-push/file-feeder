package com.pushtechnology.utils.filefeeder;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Supplier;

class ChunkSupplier implements Supplier<byte[]> {

    private final byte[] src;
    private final boolean split;

    private final ArrayList<String> lines = new ArrayList<>();

    private final Random random = new Random();

    private int idx = 0;
    private boolean end = false;

    public ChunkSupplier(byte[] src, boolean split) {
        this.src = src;
        this.split = split;

        if (split) {
            BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(src)));
            String line;
            try {
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
            }
            catch(IOException ex) {
                System.err.println(ex);
            }
        }
    }

    @Override
    public byte[] get() {
        if(end) {
            return null;
        }

        if (split) {
            String line = lines.get(idx);
            idx++;
            if (idx >= lines.size()) {
                end = true;
            }
            return line.getBytes();
        }
        else {
            end = true;
            return src;
        }
    }

    public byte[] getRandom() {
        if(! split) {
            return null;
        }

        String line = lines.get(random.nextInt(lines.size()));
        return line.getBytes();
    }

}
