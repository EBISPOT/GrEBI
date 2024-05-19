package uk.ac.ebi.grebi;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GrebiFacetedResultsPage<T> extends PageImpl<T> {

    public Map<String, Map<String, Long>> facetFieldToCounts;

    public GrebiFacetedResultsPage(List<T> results, Map<String, Map<String, Long>> facetFieldToCounts, Pageable pageable, long numFound) {
        super(results, pageable, numFound);
        this.facetFieldToCounts = facetFieldToCounts;
    }

    public <U> GrebiFacetedResultsPage<U> map(Function<? super T, ? extends U> converter) {
        return new GrebiFacetedResultsPage<U>(getConvertedContent(converter), facetFieldToCounts, getPageable(), getTotalElements());
    }




}