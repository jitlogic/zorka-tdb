package io.zorka.tdb.search;

import io.zorka.tdb.search.ssn.StringSearchNode;
import io.zorka.tdb.search.ssn.TextNode;

public class QmiQueryBuilder extends QueryBuilder {

    private QmiNode qmi;

    QmiQueryBuilder() {
        super(new QmiNode());
        this.qmi = (QmiNode)node;
    }

    public QmiQueryBuilder env(int envId) {
        qmi.setEnvId(envId);
        return this;
    }

    public QmiQueryBuilder app(int appId) {
        qmi.setAppId(appId);
        return this;
    }

    public QmiQueryBuilder type(int typeId) {
        qmi.setTypeId(typeId);
        return this;
    }

    public QmiQueryBuilder envApp(int envId, int appId) {
        qmi.setEnvId(envId);
        qmi.setAppId(appId);
        return this;
    }

    public QmiQueryBuilder minDuration(long min) {
        qmi.setMinDuration(min);
        return this;
    }

    public QmiQueryBuilder duration(long min, long max) {
        qmi.setMinDuration(min);
        qmi.setMaxDuration(max);
        return this;
    }

    public QmiQueryBuilder tstart(long tstart) {
        qmi.setTstart(tstart);
        return this;
    }

    public QmiQueryBuilder tstop(long tstop) {
        qmi.setTstop(tstop);
        return this;
    }

    public QmiQueryBuilder timespan(long tstart, long tstop) {
        qmi.setTstart(tstart);
        qmi.setTstop(tstop);
        return this;
    }

    public QmiQueryBuilder error(boolean errorFlag) {
        qmi.setErrorFlag(errorFlag);
        return this;
    }

    public QmiQueryBuilder minCalls(long minCalls) {
        qmi.setMinCalls(minCalls);
        return this;
    }

    public QmiQueryBuilder calls(long minCalls, long maxCalls) {
        qmi.setMinCalls(minCalls);
        qmi.setMaxDuration(maxCalls);
        return this;
    }

    public QmiQueryBuilder minRecs(long minRecs) {
        qmi.setMinRecs(minRecs);
        return this;
    }

    public QmiQueryBuilder recs(long minRecs, long maxRecs) {
        qmi.setMinRecs(minRecs);
        qmi.setMaxRecs(maxRecs);
        return this;
    }

    public QmiQueryBuilder minErrors(long minErrors) {
        qmi.setMinErrs(minErrors);
        return this;
    }

    public QmiQueryBuilder errors(long minErrors, long maxErrors) {
        qmi.setMinErrs(minErrors);
        qmi.setMaxRecs(maxErrors);
        return this;
    }

    public QmiQueryBuilder uuid(String uuid) {
        qmi.setUuid(uuid);
        return this;
    }

    public QmiQueryBuilder desc(String desc) {
        qmi.setDesc(new TextNode(desc.getBytes(), true, true));
        return this;
    }

    public QmiQueryBuilder desc(StringSearchNode node) {
        qmi.setDesc(node);
        return this;
    }
}
