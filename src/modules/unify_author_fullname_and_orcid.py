import pandas as pd

def unify_author_fullname_and_orcid_core(
    df,
    orcid_col='Orcid',
    fullname_col='AuthorFullName',
    no_orcid_value='NO ORCID'
):
    """
    The original core function that does two steps:
      1) unify name by ORCID (pick the longest name),
      2) unify ORCID by the newly unified name (pick the most frequent ORCID, ignoring 'NO ORCID').

    Returns a DataFrame with 'UnifiedName' and 'UnifiedOrcid'.
    """
    df['Orcid'] = df['Orcid'].fillna('NO ORCID')# Remove '.' at the end of strings in a specific column (e.g., 'ColumnName')
    df['AuthorFullName'] = df['AuthorFullName'].str.rstrip('.')
    
    def pick_longest_name(names_series):
        freq = names_series.value_counts(dropna=False)
        freq_df = freq.reset_index()
        freq_df.columns = ['name', 'count']
        freq_df['length'] = freq_df['name'].str.len()

        # Sort by 'length' descending, then 'count' descending
        freq_df = freq_df.sort_values(by=['length', 'count'], ascending=[False, False])
        return freq_df['name'].iloc[0]

    # 1) Group by ORCID → unify name
    best_name_per_orcid = (
        df.groupby(orcid_col)[fullname_col].agg(pick_longest_name)
    )
    df['UnifiedName'] = df[orcid_col].map(best_name_per_orcid)

    def pick_most_frequent_orcid(orcids_series):
        freq = orcids_series.value_counts(dropna=False)
        if no_orcid_value in freq.index:
            freq = freq.drop(no_orcid_value)

        if freq.empty:
            return no_orcid_value
        else:
            return freq.idxmax()

    # 2) Group by UnifiedName → unify ORCID
    best_orcid_per_name = (
        df.groupby('UnifiedName')[orcid_col].agg(pick_most_frequent_orcid)
    )
    df['UnifiedOrcid'] = df['UnifiedName'].map(best_orcid_per_name)

    return df


def unify_author_fullname_and_orcid(
    wos_authors_enriched: pd.DataFrame,
    authorname_col='AuthorName',
    fullname_col='AuthorFullName',
    orcid_col='Orcid',
    max_unique_fullnames=5
):
    """
    Wrapper function that:
      - Finds each distinct AuthorName (except 'ANONYMOUS').
      - For each AuthorName, checks how many distinct AuthorFullName.
      - If <= max_unique_fullnames, applies the normal unification logic.
      - If > max_unique_fullnames, applies a fallback approach:
          * e.g. 'UnifiedName' = the original AuthorFullName
          * e.g. 'UnifiedOrcid' = the original ORCID

    Finally, concatenates all results together and returns a single DataFrame
    with new columns 'UnifiedName' and 'UnifiedOrcid'.
    """

    # We will accumulate results in a list, then concat them at the end
    all_results = []

    # 1) Identify all unique authorName values, sorted by frequency (descending) if you want
    # Exclude 'ANONYMOUS' from the loop
    authorname_counts = wos_authors_enriched[authorname_col].value_counts()
    unique_authornames = authorname_counts.index.tolist()

    for short_name in unique_authornames:
        # If it's ANONYMOUS, skip normal logic but keep them as-is
        if short_name.upper() == 'ANONYMOUS':
            # Just copy them out
            df_tmp = wos_authors_enriched[
                wos_authors_enriched[authorname_col] == short_name
            ].copy()

            # For ANONYMOUS, let's just keep them as-is.
            # or set the UnifiedName = AuthorFullName
            df_tmp['UnifiedName'] = df_tmp[fullname_col]
            df_tmp['UnifiedOrcid'] = df_tmp[orcid_col]
            all_results.append(df_tmp)
            continue

        # Filter the subset
        subset = wos_authors_enriched[
            wos_authors_enriched[authorname_col] == short_name
        ].copy()

        # 2) Count how many distinct AuthorFullName
        num_distinct = subset[fullname_col].nunique()

        # 3) Decide on the approach
        if num_distinct <= max_unique_fullnames:
            # Use the normal unify logic
            subset_result = unify_author_fullname_and_orcid_core(
                df=subset,
                orcid_col=orcid_col,
                fullname_col=fullname_col
            )
        else:
            # fallback approach
            # e.g. no merging: for each row, 'UnifiedName' = 'AuthorFullName'
            # and 'UnifiedOrcid' = 'Orcid'
            subset_result = subset.copy()
            subset_result['UnifiedName'] = subset_result[fullname_col]
            subset_result['UnifiedOrcid'] = subset_result[orcid_col]

        all_results.append(subset_result)

    # 4) Combine all subsets
    final_df = pd.concat(all_results, ignore_index=True)
    final_df['UnifiedOrcid'] = final_df['UnifiedOrcid'].fillna('NO ORCID')
    final_df['UnifiedName'] = final_df['UnifiedName'].str.rstrip('.')
    # Create the AuthorID column by concatenating AuthorName, AuthorFullName, and UnifiedOrcid
    wos_author_enriched_1['AuthorID'] = (
        wos_author_enriched_1['AuthorName'] + '_' +
        wos_author_enriched_1['AuthorFullName'] + '_' +
        wos_author_enriched_1['UnifiedOrcid']
    )
    # Clean the AuthorID column: remove special characters and replace spaces with underscores
    wos_author_enriched_1['AuthorID'] = (
        wos_author_enriched_1['AuthorID']
        .str.replace(r'[.,]', '', regex=True)  # Remove '.' and ','
        .str.replace(r'\s+', '_', regex=True)  # Replace spaces with '_'
    )


    return final_df