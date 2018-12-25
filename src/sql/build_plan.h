#ifndef BUILD_PLAN_H
#define BUILD_PLAN_H

#include "parse_node.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int resolve(ResultPlan* result_plan, ParseNode* node);
extern void destroy_plan(ResultPlan* result_plan);

#ifdef __cplusplus
}
#endif

#endif //BUILD_PLAN_H
