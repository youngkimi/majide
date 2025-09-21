// majide-bootstrap/src/main/java/net/majide/bootstrap/catalog/CatalogRegistrar.java
package net.majide.bootstrap.catalog;

import net.majide.bootstrap.props.MajideProperties;
import net.majide.core.model.Task;
import net.majide.core.spi.JobRepository;
import net.majide.core.spi.TaskDependencyRepository;
import net.majide.core.spi.TaskRepository;
import net.majide.core.spi.TxRunner;
import net.majide.integration.spring.cron.CronSlotPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class CatalogRegistrar {
    private static final Logger log = LoggerFactory.getLogger(CatalogRegistrar.class);

    private final JobRepository jobs;
    private final TaskRepository tasks;
    private final TaskDependencyRepository deps;
    private final TxRunner tx;
    private final ZoneId zone;

    public CatalogRegistrar(JobRepository jobs,
                            TaskRepository tasks,
                            TaskDependencyRepository deps,
                            TxRunner tx,
                            ZoneId zone) {
        this.jobs = jobs;
        this.tasks = tasks;
        this.deps = deps;
        this.tx = tx;
        this.zone = zone;
    }

    public void register(MajideProperties.Catalog catalog) throws Exception {
        for (var j : catalog.getJobs()) {
            upsertJobAndTasks(j);
        }
    }

    private void upsertJobAndTasks(MajideProperties.JobDef def) throws Exception {
        if (def.getName() == null || def.getCronExpr() == null) {
            throw new IllegalArgumentException("job.name and job.cronExpr are required");
        }

        // 1) JOB upsert (NEXT_DUE_AT = next slot)
        var now = Instant.now();
        var slot = CronSlotPlanner.compute(def.getCronExpr(), zone, now);
        var job = tx.required(() -> jobs.upsert(
                def.getName(),
                def.getDescription(),
                def.getCronExpr(),
                slot.nextUtc(),  // 다음 due
                true             // enabled
        ));

        // 2) TASK upsert & ID 맵
        Map<String, Long> taskIdByName = new HashMap<>();
        tx.required(() -> {
            Instant ts = Instant.now();
            for (var t : def.getTasks()) {
                int indegree = (t.getDependsOn() == null) ? 0 : t.getDependsOn().size();
                tasks.upsert(new Task(
                        null, job.id(), t.getName(), t.getHandler(),
                        null, null,
                        null, indegree, ts, ts
                ));
                var saved = tasks.findByJobAndName(job.id(), t.getName()).orElseThrow();
                taskIdByName.put(t.getName(), saved.id());
            }
            return null;
        });

        // 3) DEP 등록 (멱등)
        tx.required(() -> {
            for (var t : def.getTasks()) {
                var postId = taskIdByName.get(t.getName());
                if (t.getDependsOn() != null) {
                    for (var preName : t.getDependsOn()) {
                        var preId = taskIdByName.get(preName);
                        if (preId == null) throw new IllegalStateException("Unknown dependsOn: " + preName);
                        deps.add(preId, postId);
                    }
                }
            }
            return null;
        });

        log.info("Catalog registered: job='{}' tasks={}", def.getName(), def.getTasks().size());
    }
}
