import { Parser } from 'node-sql-parser';
import { sync, Graph, ApiProperties } from 'ardoq-sdk-js';

type ArdoqCubeGraph = Graph<{}, {}>;

type Cube = {
  name: string;
  sql: () => string;
};

type Edge = {
  from: string;
  to: string;
};

export const getGraphFromCube = (metaTransformer: any): ArdoqCubeGraph => {
  const joinGraph = metaTransformer.joinGraph;
  const evaluatedCubes = metaTransformer.cubeEvaluator.evaluatedCubes;
  const sqlParser = new Parser();
  const cubeComponents = Object.entries(evaluatedCubes).map(
    ([cubeKey, cube]: [string, Cube]) => {
      return {
        customId: cubeKey,
        workspace: 'cubes',
        name: cube.name,
        type: 'Cube',
        fields: [],
      };
    }
  );
  const visitedTables = new Set();
  const references = Object.entries(joinGraph.edges).map(
    ([edgeKey, edge]: [string, Edge]) => ({
      customId: edgeKey,
      source: edge.from,
      target: edge.to,
      type: 'Joins',
    })
  );
  const tableComponents = Object.values(evaluatedCubes).flatMap(
    (cube: Cube) => {
      try {
        const parsedSql = sqlParser.astify(cube.sql());
        if (!Array.isArray(parsedSql) && parsedSql.type === 'select') {
          return parsedSql.from
            .map(visitedTable => {
              // eslint-disable-next-line @typescript-eslint/ban-ts-ignore
              // @ts-ignore
              const tableKey = `${visitedTable.db}.${visitedTable.table}`;
              references.push({
                customId: `${cube.name}-${tableKey}`,
                source: cube.name,
                target: tableKey,
                type: 'SELECT',
              });
              if (!visitedTables.has(tableKey)) {
                visitedTables.add(tableKey);
                return {
                  customId: tableKey,
                  workspace: 'cubes',
                  name: tableKey,
                  type: 'Table',
                  fields: [],
                };
              }
            })
            .filter(Boolean);
        }
      } catch (e) {
        console.log(`ERROR WHEN PARSING ${cube.name}`, cube);
      }
      return [];
    }
  );
  return {
    components: [...cubeComponents, ...tableComponents],
    references,
  };
};

export const syncCubeGraphToArdoq = (
  graph: ArdoqCubeGraph,
  apiProps: ApiProperties,
  workspaces: Record<string, string>
) => {
  sync(apiProps, workspaces, graph, []);
};
