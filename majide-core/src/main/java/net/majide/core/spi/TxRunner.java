package net.majide.core.spi;

import java.util.concurrent.Callable;

public interface TxRunner {
    <T> T required(Callable<T> body) throws Exception;
    <T> T requiresNew(Callable<T> body) throws Exception;
    default void required(Runnable body) throws Exception { required(() -> { body.run(); return null; }); }
    default void requiresNew(Runnable body) throws Exception { requiresNew(() -> { body.run(); return null; }); }
}