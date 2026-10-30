#!/usr/bin/env python
"""Add the ecan_knowledge types, fields, data sources and resolvers to the existing AppSync API.  ADDITIONS ONLY.

    python tools/appsync_add.py            # dry run: show exactly what would be added; change nothing
    python tools/appsync_add.py --apply    # push the merged schema, then create/refresh OUR data sources and resolvers

Deploy the CDK stack first (`cdk deploy`): the resolvers point at its Lambda and role.

Safety rules (same spirit as eCan_lambda/us/tools/appsync_apply.py):
  * The base is the LIVE schema (introspection), never a file, so nothing deployed by anyone else can be lost.
  * appsync/additions.graphql is merged in: new types appended, new Query/Mutation/Subscription fields inserted.
  * Refuses if any live field would disappear or change, if an addition collides with a live definition that differs,
    or if the merged schema does not parse/validate.  Additions already live and identical are skipped (re-runnable).
  * Only data sources named ecanKnowledge* and resolvers on fields from additions.graphql are created or updated.
"""
import copy
import dataclasses
import os
import re
import sys
import time
from pathlib import Path

import boto3
from graphql import build_ast_schema, parse, print_ast
from graphql.language import ObjectTypeDefinitionNode

ROOT = Path(__file__).resolve().parent.parent
ADDITIONS = ROOT / "appsync" / "additions.graphql"
API_ID = os.environ.get("APPSYNC_API_ID", "ydusqd3wgfb6loiu2daej6qa6y")        # "readScreen"
STACK = os.environ.get("STACK_NAME", "EcanListing")
ECAN_LAMBDA_SCHEMA = Path(os.environ.get(
    "ECAN_LAMBDA_SCHEMA", ROOT.parent / "eCan_lambda/us/aws/agentScheduler/graphql/agent.graphql"))
ROOT_TYPES = ("Query", "Mutation", "Subscription")
DS_API, DS_EVENTS = "ecanKnowledgeApi", "ecanKnowledgeEvents"
NONE_RESOLVERS = {("Mutation", "publishListingJobEvent")}       # local pass-through: the publish feeds the subscription
PASS_REQUEST = '{"version": "2018-05-29", "payload": $util.toJson($context.arguments.input)}'
PASS_RESPONSE = "$util.toJson($context.result)"

# Declarations AppSync provides implicitly; needed only to validate the merged schema locally.
AWS_PRELUDE = """
scalar AWSJSON scalar AWSDateTime scalar AWSDate scalar AWSTime scalar AWSTimestamp scalar AWSEmail scalar AWSURL
scalar AWSPhone scalar AWSIPAddress
directive @aws_api_key on OBJECT | FIELD_DEFINITION
directive @aws_iam on OBJECT | FIELD_DEFINITION
directive @aws_lambda on OBJECT | FIELD_DEFINITION
directive @aws_oidc on OBJECT | FIELD_DEFINITION
directive @aws_cognito_user_pools(cognito_groups: [String]) on OBJECT | FIELD_DEFINITION
directive @aws_auth(cognito_groups: [String]) on FIELD_DEFINITION
directive @aws_subscribe(mutations: [String]) on FIELD_DEFINITION
"""


def norm(node) -> str:
    """Comparable text of a definition or field: printed AST without descriptions, whitespace-collapsed."""
    if getattr(node, "description", None) is not None:
        if dataclasses.is_dataclass(node):              # graphql-core >= 3.3 nodes are frozen dataclasses
            node = dataclasses.replace(node, description=None)
        else:
            node = copy.copy(node)
            node.description = None
    return re.sub(r"\s+", " ", print_ast(node)).strip()


def defs(sdl: str) -> dict:
    """{'TypeName': node} for every definition; {'Type.field': node} for the fields of every type and input."""
    out = {}
    for d in parse(sdl).definitions:
        name = getattr(getattr(d, "name", None), "value", None)
        if not name:
            continue
        out[name] = d
        for f in getattr(d, "fields", None) or []:
            out[f"{name}.{f.name.value}"] = f
    return out


def field_signatures(sdl: str) -> dict:
    return {k: norm(v) for k, v in defs(sdl).items() if "." in k}


def merge(live_sdl: str, add_sdl: str):
    """Return (merged_sdl, added_names, skipped_names) or raise SystemExit with the reason for refusing."""
    live = defs(live_sdl)
    doc = parse(add_sdl)
    src = add_sdl
    new_types, new_fields, added, skipped, problems = [], {t: [] for t in ROOT_TYPES}, [], [], []
    for d in doc.definitions:
        name = d.name.value
        text = src[d.loc.start:d.loc.end]
        if name in ROOT_TYPES and isinstance(d, ObjectTypeDefinitionNode):
            for f in d.fields:
                key = f"{name}.{f.name.value}"
                ftext = src[f.loc.start:f.loc.end]
                if key in live:
                    (skipped if norm(live[key]) == norm(parse(f"type X {{ {ftext} }}").definitions[0].fields[0])
                     else problems).append(key)
                else:
                    new_fields[name].append(ftext)
                    added.append(key)
            continue
        if name in live:
            (skipped if norm(live[name]) == norm(parse(text).definitions[0]) else problems).append(name)
        else:
            new_types.append(text)
            added.append(name)
    if problems:
        sys.exit("REFUSING: these names already exist in the live schema with a DIFFERENT definition:\n   "
                 + "\n   ".join(problems))

    lines = live_sdl.replace("\r\n", "\n").split("\n")
    for t, fields in new_fields.items():
        if not fields:
            continue
        start = next((i for i, ln in enumerate(lines) if re.match(rf"^type {t}\b[^{{]*\{{\s*$", ln)), None)
        if start is None:
            sys.exit(f"REFUSING: live schema has no 'type {t} {{' block to add fields to")
        end = next(i for i in range(start + 1, len(lines)) if re.match(r"^\}\s*$", lines[i]))
        lines[end:end] = ["  " + f for f in fields]
    merged = "\n".join(lines).rstrip("\n") + "\n"
    if new_types:
        merged += "\n# ---- ecan_knowledge (github.com/scszcoder/ecan_knowledge, appsync/additions.graphql) ----\n\n"
        merged += "\n\n".join(new_types) + "\n"
    return merged, added, skipped


def check_preserved(live_sdl: str, merged_sdl: str) -> None:
    before, after = field_signatures(live_sdl), field_signatures(merged_sdl)
    lost = sorted(k for k in before if k not in after)
    changed = sorted(k for k in before if k in after and before[k] != after[k])
    if lost or changed:
        sys.exit(f"REFUSING: the merge would remove {lost} or change {changed}")
    names = [getattr(getattr(d, "name", None), "value", None) for d in parse(merged_sdl).definitions]
    dup = sorted({n for n in names if n and names.count(n) > 1})
    if dup:
        sys.exit(f"REFUSING: defined more than once: {dup}")
    try:
        build_ast_schema(parse(AWS_PRELUDE + merged_sdl))
    except Exception as e:                                  # an error the live schema doesn't have is ours
        try:
            build_ast_schema(parse(AWS_PRELUDE + live_sdl))
        except Exception:
            print(f"note: the live schema itself does not validate locally ({str(e)[:200]}); syntax checks only")
            return
        sys.exit(f"REFUSING: merged schema does not validate: {e}")


def stack_outputs(session) -> dict:
    try:
        st = session.client("cloudformation").describe_stacks(StackName=STACK)["Stacks"][0]
    except Exception:
        return {}
    return {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])}


def ensure_data_source(ai, name: str, apply: bool, **cfg) -> None:
    try:
        cur = ai.get_data_source(apiId=API_ID, name=name)["dataSource"]
    except ai.exceptions.NotFoundException:
        cur = None
    if cur is None:
        print(f"   + data source {name} ({cfg['type']})")
        if apply:
            ai.create_data_source(apiId=API_ID, name=name, **cfg)
        return
    same = (cur["type"] == cfg["type"] and cur.get("serviceRoleArn") == cfg.get("serviceRoleArn")
            and (cur.get("lambdaConfig") or {}).get("lambdaFunctionArn") ==
            (cfg.get("lambdaConfig") or {}).get("lambdaFunctionArn"))
    print(f"   = data source {name} {'unchanged' if same else '(ours) will be updated'}")
    if apply and not same:
        ai.update_data_source(apiId=API_ID, name=name, **cfg)


def ensure_resolver(ai, type_name: str, field: str, apply: bool) -> None:
    if (type_name, field) in NONE_RESOLVERS:
        kw = dict(dataSourceName=DS_EVENTS, requestMappingTemplate=PASS_REQUEST, responseMappingTemplate=PASS_RESPONSE)
    else:
        kw = dict(dataSourceName=DS_API)                    # direct Lambda resolver (no mapping templates)
    try:
        cur = ai.get_resolver(apiId=API_ID, typeName=type_name, fieldName=field)["resolver"]
    except ai.exceptions.NotFoundException:
        cur = None
    if cur and cur.get("dataSourceName") not in (DS_API, DS_EVENTS):
        sys.exit(f"REFUSING: {type_name}.{field} already has a resolver on someone else's data source "
                 f"({cur.get('dataSourceName')})")
    print(f"   {'=' if cur else '+'} resolver {type_name}.{field} -> {kw['dataSourceName']}")
    if apply:
        (ai.update_resolver if cur else ai.create_resolver)(apiId=API_ID, typeName=type_name, fieldName=field, **kw)


def main():
    apply = "--apply" in sys.argv
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "sc"), region_name="us-east-1")
    ai = session.client("appsync")
    live_sdl = ai.get_introspection_schema(apiId=API_ID, format="SDL", includeDirectives=True)["schema"].read().decode()
    add_sdl = ADDITIONS.read_text(encoding="utf-8")
    merged, added, skipped = merge(live_sdl, add_sdl)
    check_preserved(live_sdl, merged)
    print(f"live definitions+fields: {len(defs(live_sdl))}   existing fields modified: 0   removed: 0")
    print(f"already live (identical, skipped): {len(skipped)}")
    print(f"{len(added)} to add:")
    for a in added:
        print("   +", a)

    ours = [(t, f.name.value) for d in parse(add_sdl).definitions if d.name.value in ROOT_TYPES
            for t, f in [(d.name.value, f) for f in d.fields]]
    in_repo = ECAN_LAMBDA_SCHEMA.exists() and all(
        f"{t}.{f}" in defs(ECAN_LAMBDA_SCHEMA.read_text(encoding="utf-8")) for t, f in ours)
    if not in_repo:
        print(f"\nWARNING: {ECAN_LAMBDA_SCHEMA} lacks these fields; eCan_lambda's appsync_apply.py would refuse to run "
              "after this push until the additions block is copied there.")

    out = stack_outputs(session)
    if not out.get("ApiFunctionArn"):
        sys.exit(f"\nstack {STACK} is not deployed (no ApiFunctionArn output): run `cdk deploy` first")
    print("\ndata sources and resolvers (ours only):")
    if apply and added:
        r = ai.start_schema_creation(apiId=API_ID, definition=merged.encode())
        print("schema push:", r["status"])
        for _ in range(100):
            st = ai.get_schema_creation_status(apiId=API_ID)
            if st["status"] in ("SUCCESS", "ACTIVE", "FAILED", "NOT_APPLICABLE"):
                break
            time.sleep(3)
        print("schema status:", st["status"], st.get("details", ""))
        if st["status"] not in ("SUCCESS", "ACTIVE"):
            sys.exit("schema push did not succeed; no data source or resolver was changed")
        now = ai.get_introspection_schema(apiId=API_ID, format="SDL", includeDirectives=True)["schema"].read().decode()
        missing = [k for k in list(field_signatures(live_sdl)) + [a for a in added if "." in a]
                   if k not in field_signatures(now)]
        if missing:
            sys.exit(f"after the push these fields are missing (investigate before anything else): {missing}")
        print("verified: every previously live field is still there, and the additions are live")
    ensure_data_source(ai, DS_API, apply, type="AWS_LAMBDA", serviceRoleArn=out["AppSyncRoleArn"],
                       lambdaConfig={"lambdaFunctionArn": out["ApiFunctionArn"]})
    ensure_data_source(ai, DS_EVENTS, apply, type="NONE")
    for t, f in ours:
        ensure_resolver(ai, t, f, apply)
    print("\ndone" if apply else "\n(dry run) pass --apply to make these changes")


if __name__ == "__main__":
    main()
