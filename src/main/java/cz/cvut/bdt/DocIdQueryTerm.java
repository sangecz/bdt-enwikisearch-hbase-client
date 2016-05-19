package cz.cvut.bdt;

/**
 * Created by sange on 17/05/16.
 */
public class DocIdQueryTerm {

    private String docId;
    private String queryTerm;

    public DocIdQueryTerm(String doc_id, String query_term) {
        docId = doc_id;
        queryTerm = query_term;
    }

    public String getDocId() {
        return docId;
    }

    public String getQueryTerm() {
        return queryTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DocIdQueryTerm that = (DocIdQueryTerm) o;

        if (!docId.equals(that.docId)) return false;
        return queryTerm.equals(that.queryTerm);

    }

    @Override
    public int hashCode() {
        int result = docId.hashCode();
        result = 31 * result + queryTerm.hashCode();
        return result;
    }
}
