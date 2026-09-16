//! Splitting of the multi-valued GWAS Catalog columns.
//!
//! The catalog packs lists into single tab-separated cells. The plain lists
//! (MAPPED_TRAIT_URI, SNP_GENE_IDS, ...) are ", "-delimited; MAPPED_GENE also
//! encodes structure with other delimiters, see `split_mapped_genes`.

/// Split a ", "-delimited catalog list into its trimmed, non-empty items.
pub fn split_list(s: &str) -> Vec<&str> {
    s.split(", ").map(str::trim).filter(|t| !t.is_empty()).collect()
}

/// Flatten a MAPPED_GENE value into its distinct gene symbols, in first-seen order.
///
/// The column packs structure into one string, and the forms nest:
///
/// * `A, B`  genes the variant lies within
/// * `A - B` intergenic variant: nearest upstream and downstream genes
/// * `A x B` SNP x SNP interaction: the genes of each SNP
/// * `A; B`  multi-SNP haplotype: the genes of each SNP, with the placeholder
///   `No mapped genes` for SNPs that have none
///
/// e.g. `PSORS1C3, POU5F1 x HLA-C - USP8P1` or
/// `No mapped genes; CLEC18B - GLG1; No mapped genes`. Only the symbols are
/// kept: which one is upstream or downstream is carried, as Ensembl ids, by the
/// UPSTREAM_GENE_ID / DOWNSTREAM_GENE_ID / SNP_GENE_IDS columns for single-variant
/// rows, and those are the values that link to gene nodes. Hyphens inside a
/// symbol (`HLA-C`, `NOVA1-DT`) are untouched because the intergenic delimiter is
/// ` - ` with the surrounding spaces.
pub fn split_mapped_genes(s: &str) -> Vec<&str> {
    let mut symbols: Vec<&str> = Vec::new();
    let tokens = s
        .split(';')
        .flat_map(|t| t.split(" x "))
        .flat_map(|t| t.split(','))
        .flat_map(|t| t.split(" - "))
        .map(str::trim);
    for token in tokens {
        if token.is_empty() || token == "No mapped genes" || symbols.contains(&token) {
            continue;
        }
        symbols.push(token);
    }
    symbols
}

/// The MAPPED_TRAIT label for each of `n_uris` MAPPED_TRAIT_URI values.
///
/// MAPPED_TRAIT lists the labels in the same order and with the same ", "
/// delimiter as the URIs, but a label can itself contain ", " (`osteoarthritis,
/// hip`), which makes the split ambiguous. The split is trusted only when it
/// yields exactly one label per URI; otherwise every URI gets the whole string
/// rather than a wrong fragment.
pub fn trait_labels(mapped_trait: &str, n_uris: usize) -> Vec<&str> {
    let labels = split_list(mapped_trait);
    if labels.len() == n_uris {
        labels
    } else {
        vec![mapped_trait; n_uris]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_splits_on_comma_space_and_drops_empties() {
        assert_eq!(split_list(""), Vec::<&str>::new());
        assert_eq!(split_list("ENSG1"), vec!["ENSG1"]);
        assert_eq!(split_list("ENSG1, ENSG2"), vec!["ENSG1", "ENSG2"]);
        assert_eq!(
            split_list("http://www.ebi.ac.uk/efo/EFO_0000400, http://www.ebi.ac.uk/efo/EFO_0009924"),
            vec!["http://www.ebi.ac.uk/efo/EFO_0000400", "http://www.ebi.ac.uk/efo/EFO_0009924"]
        );
    }

    #[test]
    fn single_and_overlapping_genes() {
        assert_eq!(split_mapped_genes(""), Vec::<&str>::new());
        assert_eq!(split_mapped_genes("GCKR"), vec!["GCKR"]);
        assert_eq!(split_mapped_genes("HBE1, HBG2"), vec!["HBE1", "HBG2"]);
    }

    #[test]
    fn intergenic_pair_keeps_hyphenated_symbols() {
        // issue #12: the ` - ` between the nearest upstream and downstream gene
        assert_eq!(split_mapped_genes("LMCD1-AS1 - GRM7"), vec!["LMCD1-AS1", "GRM7"]);
        assert_eq!(split_mapped_genes("NOVA1-DT, MIR4307HG"), vec!["NOVA1-DT", "MIR4307HG"]);
    }

    #[test]
    fn interaction_nests_the_other_forms() {
        assert_eq!(split_mapped_genes("UPF2 x SPINT2"), vec!["UPF2", "SPINT2"]);
        assert_eq!(
            split_mapped_genes("PSORS1C3, POU5F1 x HLA-C - USP8P1"),
            vec!["PSORS1C3", "POU5F1", "HLA-C", "USP8P1"]
        );
    }

    #[test]
    fn haplotype_drops_placeholder_and_duplicates() {
        assert_eq!(
            split_mapped_genes("No mapped genes; CLEC18B - GLG1; No mapped genes"),
            vec!["CLEC18B", "GLG1"]
        );
        assert_eq!(
            split_mapped_genes("IGHV4-61; IGHV4-61; IGHV4-61; IGHV3-62 - IGHVII-62-1"),
            vec!["IGHV4-61", "IGHV3-62", "IGHVII-62-1"]
        );
        // the catalog leaves a trailing "; " on some haplotype rows
        assert_eq!(split_mapped_genes("CASC19; PCAT1; CASC19; "), vec!["CASC19", "PCAT1"]);
    }

    #[test]
    fn trait_labels_pair_with_uris_when_counts_match() {
        assert_eq!(trait_labels("diabetes mellitus", 1), vec!["diabetes mellitus"]);
        assert_eq!(
            trait_labels("diabetes mellitus, Drugs used in diabetes use measurement", 2),
            vec!["diabetes mellitus", "Drugs used in diabetes use measurement"]
        );
    }

    #[test]
    fn trait_labels_fall_back_to_whole_string_when_ambiguous() {
        assert_eq!(trait_labels("osteoarthritis, hip", 1), vec!["osteoarthritis, hip"]);
        assert_eq!(
            trait_labels("osteoarthritis, hip, osteoarthritis, knee", 2),
            vec!["osteoarthritis, hip, osteoarthritis, knee"; 2]
        );
        assert_eq!(trait_labels("", 1), vec![""]);
        assert_eq!(trait_labels("", 0), Vec::<&str>::new());
    }
}
