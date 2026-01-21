def get_tos_df(tos_papers_merged):
    """
    Processes the merged DataFrame to produce a final DataFrame with the specified ordering and columns.
    Roots and trunks are removed from branches.

    Parameters:
    ----------
    tos_papers_merged : pd.DataFrame
        DataFrame containing 'AU', 'TI', 'PY', 'DI', 'tos', and 'index' columns.

    Returns:
    -------
    pd.DataFrame
        Final DataFrame with columns ['No', 'AU', 'TI', 'PY', 'DI', 'tos'],
        ordered according to the specified logic.
    """

    import pandas as pd

    # Step 1: Ensure 'PY' is numeric
    tos_papers_merged['PY'] = pd.to_numeric(tos_papers_merged['PY'], errors='coerce')

    # Step 2: Select only the necessary columns
    tos_papers_selected = tos_papers_merged[['index', 'AU', 'TI', 'PY', 'DI', 'tos']].copy()

    # Step 3: Remove roots and trunks from branches
    # Ensure that roots and trunks are not included in branches
    branches_df = tos_papers_selected[
        tos_papers_selected['tos'].str.startswith('branch_') &
        (~tos_papers_selected['tos'].isin(['root', 'trunk']))
    ].copy()

    # Extract roots and trunks separately
    root_df = tos_papers_selected[tos_papers_selected['tos'] == 'root'].copy()
    trunk_df = tos_papers_selected[tos_papers_selected['tos'] == 'trunk'].copy()

    # Step 4: Sort 'root' and 'trunk' DataFrames by 'PY' in ascending order (oldest first)
    root_df_sorted = root_df.sort_values(by='PY', ascending=True)
    trunk_df_sorted = trunk_df.sort_values(by='PY', ascending=True)

    # Step 5: Process branches
    # Get list of unique branches and sort them by branch number
    branch_tos_values = branches_df['tos'].unique()
    branch_numbers = []
    for branch_tos in branch_tos_values:
        branch_num = int(branch_tos.split('_')[1])
        branch_numbers.append((branch_num, branch_tos))

    # Sort branches by branch number
    branch_numbers.sort()

    # For each branch, sort by 'PY' in descending order (newest first)
    branch_dfs = []
    for branch_num, branch_tos in branch_numbers:
        branch_df = branches_df[branches_df['tos'] == branch_tos].copy()
        branch_df_sorted = branch_df.sort_values(by='PY', ascending=False)
        branch_dfs.append(branch_df_sorted)

    # Step 6: Concatenate all DataFrames in the specified order
    dfs_in_order = [root_df_sorted, trunk_df_sorted] + branch_dfs
    final_df = pd.concat(dfs_in_order, ignore_index=True)

    # Step 7: Add 'No' column starting from 1 to the number of total rows
    final_df.insert(0, 'No', range(1, len(final_df) + 1))

    # Step 8: Select and reorder the required columns
    final_df = final_df[['No', 'AU', 'TI', 'PY', 'DI', 'tos']]

    return final_df
