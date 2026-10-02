import { boolLiteral, quoteIdentifier } from "./dbCompat";
import { executeRaw, queryRaw } from "./dbRuntime";
import { mapWithConcurrency } from "./asyncPool";

export type ForwardGroupTemplateHostReferenceRow = {
  ruleId: number;
  groupId: number;
  storedHostId: number;
  candidateHostId: number | null;
  stillReferenced: boolean | number | string;
};

function positiveId(value: unknown) {
  const id = Math.floor(Number(value || 0));
  return Number.isInteger(id) && id > 0 ? id : 0;
}

function dbBool(value: unknown) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return value === true || value === 1 || normalized === "1" || normalized === "true";
}

export function forwardGroupTemplateHostRepairs(rows: ForwardGroupTemplateHostReferenceRow[]) {
  const ordered = [...rows].sort((left, right) => Number(left.ruleId) - Number(right.ruleId));
  const repairs: Array<{ ruleId: number; groupId: number; fromHostId: number; toHostId: number }> = [];
  const seenRuleIds = new Set<number>();
  for (const row of ordered) {
    const ruleId = positiveId(row.ruleId);
    const groupId = positiveId(row.groupId);
    const fromHostId = positiveId(row.storedHostId);
    const toHostId = positiveId(row.candidateHostId);
    if (!ruleId || !groupId || !fromHostId || !toHostId || seenRuleIds.has(ruleId) || dbBool(row.stillReferenced)) continue;
    seenRuleIds.add(ruleId);
    if (fromHostId !== toHostId) repairs.push({ ruleId, groupId, fromHostId, toHostId });
  }
  return repairs;
}

/**
 * Rebinds group-rule templates whose stored host is no longer an actual group
 * member. The hostId on a template is a UI/runtime anchor, not a listener;
 * generated child rules and current membership remain the real references.
 */
export async function repairForwardGroupTemplateHostReferences(options: {
  hostId?: unknown;
  groupId?: unknown;
} = {}) {
  const requestedHostId = options.hostId === undefined ? 0 : positiveId(options.hostId);
  const requestedGroupId = options.groupId === undefined ? 0 : positiveId(options.groupId);
  if (options.hostId !== undefined && requestedHostId <= 0) return [];
  if (options.groupId !== undefined && requestedGroupId <= 0) return [];

  const q = quoteIdentifier;
  const where = [
    `r.${q("isForwardGroupTemplate")} = ${boolLiteral(true)}`,
    `r.${q("pendingDelete")} = ${boolLiteral(false)}`,
  ];
  const params: number[] = [];
  if (requestedHostId > 0) {
    where.push(`r.${q("hostId")} = ?`);
    params.push(requestedHostId);
  }
  if (requestedGroupId > 0) {
    where.push(`g.${q("id")} = ?`);
    params.push(requestedGroupId);
  }

  const hostEndpoint = (memberAlias: string, tunnelAlias: string) => `CASE
    WHEN ${memberAlias}.${q("memberType")} = 'host' THEN ${memberAlias}.${q("hostId")}
    WHEN ${memberAlias}.${q("memberType")} = 'tunnel' THEN ${tunnelAlias}.${q("entryHostId")}
    ELSE NULL
  END`;
  const endpoint = hostEndpoint("m", "t");
  const rows = await queryRaw<ForwardGroupTemplateHostReferenceRow>(
    `SELECT
        r.${q("id")} AS ${q("ruleId")},
        g.${q("id")} AS ${q("groupId")},
        r.${q("hostId")} AS ${q("storedHostId")},
        COALESCE(
          CASE WHEN g.${q("groupMode")} = 'chain'
                 AND eg.${q("groupMode")} = 'entry'
                 AND eg.${q("isEnabled")} = ${boolLiteral(true)}
            THEN (
              SELECT em.${q("hostId")}
                FROM ${q("forward_group_members")} em
               WHERE em.${q("groupId")} = eg.${q("id")}
                 AND em.${q("memberType")} = 'host'
                 AND em.${q("hostId")} IS NOT NULL
                 AND em.${q("isEnabled")} = ${boolLiteral(true)}
               ORDER BY em.${q("priority")} ASC, em.${q("id")} ASC
               LIMIT 1
            )
            ELSE NULL
          END,
          (
            SELECT ${endpoint}
              FROM ${q("forward_group_members")} m
              LEFT JOIN ${q("tunnels")} t ON m.${q("memberType")} = 'tunnel' AND t.${q("id")} = m.${q("tunnelId")}
             WHERE m.${q("groupId")} = g.${q("id")}
               AND (${endpoint}) IS NOT NULL
             ORDER BY CASE WHEN m.${q("isEnabled")} = ${boolLiteral(true)} THEN 0 ELSE 1 END,
                      m.${q("priority")} ASC, m.${q("id")} ASC
             LIMIT 1
          )
        ) AS ${q("candidateHostId")},
        CASE WHEN EXISTS (
          SELECT 1
            FROM ${q("forward_group_members")} m
            LEFT JOIN ${q("tunnels")} t ON m.${q("memberType")} = 'tunnel' AND t.${q("id")} = m.${q("tunnelId")}
           WHERE m.${q("groupId")} = g.${q("id")}
             AND (${endpoint}) = r.${q("hostId")}
        ) OR (
          g.${q("groupMode")} = 'chain'
          AND eg.${q("groupMode")} = 'entry'
          AND eg.${q("isEnabled")} = ${boolLiteral(true)}
          AND EXISTS (
            SELECT 1
              FROM ${q("forward_group_members")} em
             WHERE em.${q("groupId")} = eg.${q("id")}
               AND em.${q("memberType")} = 'host'
               AND em.${q("hostId")} = r.${q("hostId")}
               AND em.${q("isEnabled")} = ${boolLiteral(true)}
          )
        ) THEN ${boolLiteral(true)} ELSE ${boolLiteral(false)} END AS ${q("stillReferenced")}
       FROM ${q("forward_rules")} r
       INNER JOIN ${q("forward_groups")} g ON g.${q("id")} = r.${q("forwardGroupId")}
       LEFT JOIN ${q("forward_groups")} eg ON eg.${q("id")} = g.${q("entryGroupId")}
      WHERE ${where.join(" AND ")}
      ORDER BY r.${q("id")} ASC`,
    params,
  );
  const repairs = forwardGroupTemplateHostRepairs(rows);
  const updatedAt = Math.floor(Date.now() / 1000);
  await mapWithConcurrency(repairs, 12, (repair) => executeRaw(
    `UPDATE ${q("forward_rules")}
        SET ${q("hostId")} = ?, ${q("isRunning")} = ${boolLiteral(false)}, ${q("updatedAt")} = ?
      WHERE ${q("id")} = ?
        AND ${q("hostId")} = ?
        AND ${q("isForwardGroupTemplate")} = ${boolLiteral(true)}
        AND ${q("pendingDelete")} = ${boolLiteral(false)}`,
    [repair.toHostId, updatedAt, repair.ruleId, repair.fromHostId],
  ));
  return repairs;
}

export type PortForwardTemplateHostRow = {
  ruleId: number;
  storedHostId: number;
  groupId: number;
  memberId: number;
  memberHostId: number;
  memberPriority: number;
};

export function portForwardTemplateHostRepairs(rows: PortForwardTemplateHostRow[]) {
  const ordered = [...rows].sort((left, right) => (
    Number(left.ruleId) - Number(right.ruleId)
    || Number(left.memberPriority) - Number(right.memberPriority)
    || Number(left.memberId) - Number(right.memberId)
  ));
  const repairs: Array<{ ruleId: number; groupId: number; fromHostId: number; toHostId: number }> = [];
  const seenRuleIds = new Set<number>();
  for (const row of ordered) {
    const ruleId = Number(row.ruleId || 0);
    const fromHostId = Number(row.storedHostId || 0);
    const toHostId = Number(row.memberHostId || 0);
    if (!ruleId || !toHostId || seenRuleIds.has(ruleId)) continue;
    seenRuleIds.add(ruleId);
    if (fromHostId !== toHostId) {
      repairs.push({ ruleId, groupId: Number(row.groupId || 0), fromHostId, toHostId });
    }
  }
  return repairs;
}

export async function repairPortForwardRuleHostReferences(groupIdValue?: unknown) {
  const groupId = Number(groupIdValue || 0);
  const q = quoteIdentifier;
  const groupFilter = Number.isInteger(groupId) && groupId > 0 ? ` AND g.${q("id")} = ?` : "";
  const rows = await queryRaw<PortForwardTemplateHostRow>(
    `SELECT
        r.${q("id")} AS ${q("ruleId")},
        r.${q("hostId")} AS ${q("storedHostId")},
        g.${q("id")} AS ${q("groupId")},
        m.${q("id")} AS ${q("memberId")},
        m.${q("hostId")} AS ${q("memberHostId")},
        m.${q("priority")} AS ${q("memberPriority")}
       FROM ${q("forward_rules")} r
       INNER JOIN ${q("forward_groups")} g ON g.${q("id")} = r.${q("forwardGroupId")}
       INNER JOIN ${q("forward_group_members")} m ON m.${q("groupId")} = g.${q("id")}
      WHERE g.${q("groupMode")} = 'port'
        AND m.${q("memberType")} = 'host'
        AND m.${q("hostId")} IS NOT NULL
        AND r.${q("isForwardGroupTemplate")} = ${boolLiteral(true)}
        AND r.${q("pendingDelete")} = ${boolLiteral(false)}${groupFilter}
      ORDER BY r.${q("id")} ASC, m.${q("priority")} ASC, m.${q("id")} ASC`,
    groupFilter ? [groupId] : [],
  );
  const repairs = portForwardTemplateHostRepairs(rows);
  const updatedAt = Math.floor(Date.now() / 1000);
  await mapWithConcurrency(repairs, 12, (repair) => executeRaw(
    `UPDATE ${q("forward_rules")}
        SET ${q("hostId")} = ?, ${q("isRunning")} = ${boolLiteral(false)}, ${q("updatedAt")} = ?
      WHERE ${q("id")} = ?`,
    [repair.toHostId, updatedAt, repair.ruleId],
  ));
  return repairs;
}
