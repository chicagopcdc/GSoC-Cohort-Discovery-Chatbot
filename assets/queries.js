/**
 * Convert filter obj into GQL filter format
 * @param {EmptyFilter | FilterState} filterState
 * @returns {GqlFilter}
 */
export function getGQLFilter(filterState) {
    if (
      filterState === undefined ||
      !('value' in filterState) ||
      Object.keys(filterState.value).length === 0
    )
      return undefined;
  
    const combineMode = filterState.__combineMode ?? 'AND';
    if (filterState.__type === FILTER_TYPE.COMPOSED)
      return { [combineMode]: filterState.value.map(getGQLFilter) };
  
    /** @type {GqlSimpleFilter[]} */
    const simpleFilters = [];
  
    /** @type {GqlNestedFilter[]} */
    const nestedFilters = [];
    /** @type {{ [path: string]: number }} */
    const nestedFilterIndices = {};
    let nestedFilterIndex = 0;
  
    for (const [filterKey, filterValues] of Object.entries(filterState.value)) {
      const [fieldStr, nestedFieldStr] = filterKey.split('.');
      const isNestedField = nestedFieldStr !== undefined;
      const fieldName = isNestedField ? nestedFieldStr : fieldStr;
  
      if (filterValues.__type === FILTER_TYPE.ANCHORED) {
        const parsedAnchoredFilters = parseAnchoredFilters(
          fieldName,
          filterValues,
          combineMode,
        );
        for (const { nested } of parsedAnchoredFilters) {
          if (!(nested.path in nestedFilterIndices)) {
            nestedFilterIndices[nested.path] = nestedFilterIndex;
            nestedFilters.push(
              /** @type {GqlNestedFilter} */ ({
                nested: { path: nested.path, [combineMode]: [] },
              }),
            );
            nestedFilterIndex += 1;
          }
  
          nestedFilters[nestedFilterIndices[nested.path]].nested[
            combineMode
          ].push({ AND: nested.AND });
        }
      } else {
        const simpleFilter = parseSimpleFilter(fieldName, filterValues);
  
        if (simpleFilter !== undefined) {
          if (isNestedField) {
            const path = fieldStr; // parent path
  
            if (!(path in nestedFilterIndices)) {
              nestedFilterIndices[path] = nestedFilterIndex;
              nestedFilters.push(
                /** @type {GqlNestedFilter} */ ({
                  nested: { path, [combineMode]: [] },
                }),
              );
              nestedFilterIndex += 1;
            }
  
            nestedFilters[nestedFilterIndices[path]].nested[combineMode].push(
              simpleFilter,
            );
          } else {
            simpleFilters.push(simpleFilter);
          }
        }
      }
    }
  
    return { [combineMode]: [...simpleFilters, ...nestedFilters] };
  }

  /**
 * @param {Object} args
 * @param {AnchorConfig} [args.anchorConfig]
 * @param {string} [args.anchorValue]
 * @param {{ title: string; fields: string[] }[]} args.filterTabs
 * @param {GqlFilter} [args.gqlFilter]
 */
export function getQueryInfoForAggregationOptionsData({
    anchorConfig,
    anchorValue = '',
    filterTabs,
    gqlFilter,
  }) {
    const isUsingAnchor = anchorConfig !== undefined && anchorValue !== '';
    const anchorFilterPiece = isUsingAnchor
      ? { IN: { [anchorConfig.field]: [anchorValue] } }
      : undefined;
  
    /** @type {{ [group: string]: string[]; }} */
    const fieldsByGroup = {};
    /** @type {{ [group: string]: GqlFilter; }} */
    const gqlFilterByGroup = {};
  
    for (const { title, fields } of filterTabs)
      if (isUsingAnchor && anchorConfig.tabs.includes(title)) {
        for (const field of fields) {
          const [path, fieldName] = field.split('.');
  
          if (fieldName === undefined)
            fieldsByGroup.main = [...(fieldsByGroup?.main ?? []), field];
          else {
            fieldsByGroup[path] = [...(fieldsByGroup?.[path] ?? []), field];
  
            // add gqlFilterGroup for each nested field object path
            if (!(path in gqlFilterByGroup)) {
              const combineMode = gqlFilter ? Object.keys(gqlFilter)[0] : 'AND';
              const groupGqlFilter = cloneDeep(
                gqlFilter ?? { [combineMode]: [] },
              );
  
              if (anchorValue !== '' && 'AND' in groupGqlFilter) {
                const filters = /** @type {GqlFilter[]} */ (
                  groupGqlFilter[combineMode]
                );
                const found = /** @type {GqlNestedAnchoredFilter} */ (
                  filters.find((f) => 'nested' in f && f.nested.path === path)
                );
                if (found === undefined) {
                  filters.push(
                    /** @type {GqlNestedAnchoredFilter} */ ({
                      nested: { path, AND: [anchorFilterPiece] },
                    }),
                  );
                } else if (Array.isArray(found.nested.AND)) {
                  found.nested.AND.push(anchorFilterPiece);
                }
              }
              gqlFilterByGroup[`filter_${path}`] = groupGqlFilter;
            }
          }
        }
      } else {
        fieldsByGroup.main = [...(fieldsByGroup?.main ?? []), ...fields];
      }
  
    if (fieldsByGroup.main?.length > 0) gqlFilterByGroup.filter_main = gqlFilter;
  
    return {
      fieldsByGroup,
      gqlFilterByGroup,
    };
  }
  