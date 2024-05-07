package uk.ac.ebi.grebi;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GrebiFacetedResultsPage<T> {

    public Map<String, Map<String, Long>> facetFieldToCounts;
    public List<T> elements;
    public int page;
    public int numElements;
    public long totalPages;
    public long totalElements;

    public GrebiFacetedResultsPage(List<T> results, Map<String, Map<String, Long>> facetFieldToCounts, Pageable pageable, long numFound) {
        this.facetFieldToCounts = facetFieldToCounts;
        this.elements = results;
        this.page = pageable.getPageNumber();
        this.numElements = this.elements.size();
        this.totalElements = numFound;
        this.totalPages = (long)Math.ceil( ((double) numFound) /pageable.getPageSize());
    }





}