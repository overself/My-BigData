package com.beam.project.core.coder;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class PojoCustomCoder<T> extends Coder<T> {

    private Class<T> type;

    private transient @Nullable TypeDescriptor<T> typeDescriptor;

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException,IOException {

    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        return null;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
