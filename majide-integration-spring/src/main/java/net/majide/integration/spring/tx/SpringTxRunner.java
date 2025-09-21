package net.majide.integration.spring.tx;

import net.majide.adapter.jdbc.TxContext;
import net.majide.core.spi.TxRunner;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.Callable;

public final class SpringTxRunner implements TxRunner {
    private final PlatformTransactionManager tm;
    private final DataSource ds;

    public SpringTxRunner(PlatformTransactionManager tm, DataSource ds) {
        this.tm = tm;
        this.ds = ds;
    }

    @Override
    public <T> T required(Callable<T> body) {
        return execute(TransactionDefinition.PROPAGATION_REQUIRED, body);
    }

    @Override
    public <T> T requiresNew(Callable<T> body) {
        return execute(TransactionDefinition.PROPAGATION_REQUIRES_NEW, body);
    }

    private <T> T execute(int propagation, Callable<T> body) {
        var tpl = new TransactionTemplate(tm);
        tpl.setPropagationBehavior(propagation);

        return tpl.execute(status -> {
            // 이미 TxContext가 있다면 그대로 사용 (중첩 호출)
            Connection existing = TxContext.get();
            if (existing != null) {
                try { return body.call(); }
                catch (RuntimeException re) { throw re; }
                catch (Exception e) { throw new RuntimeException(e); }
            }

            // 스프링 트랜잭션의 물리 커넥션을 끌어와 TxContext에 꽂아줌
            Connection con = DataSourceUtils.getConnection(ds);
            try {
                TxContext.set(con);
                return body.call();
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                TxContext.clear();
                DataSourceUtils.releaseConnection(con, ds); // 스프링이 관리하는 방식으로 반납
            }
        });
    }
}