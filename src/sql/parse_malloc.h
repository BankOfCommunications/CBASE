#ifndef PARSER_MALLOC_H
#define PARSER_MALLOC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* NB: Be careful!!!, it is only used in parser module */
extern void *parse_malloc(const size_t nbyte, void *malloc_pool);
extern void *parse_realloc(void *ptr, size_t nbyte, void *malloc_pool);
extern void parse_free(void *ptr);
extern char *parse_strndup(const char *str, size_t nbyte, void *malloc_pool);
extern char *parse_strdup(const char *str, void *malloc_pool);

#ifdef __cplusplus
}
#endif

#endif //PARSER_MALLOC_H

