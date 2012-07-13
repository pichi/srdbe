#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <erl_nif.h>
#include <assert.h>

#ifdef USE_NATIVE
#  define SWAP32(x) (x)
#  define SWAP64(x) (x)
#else
#  define SWAP32(x) __builtin_bswap32(x)
#  define SWAP64(x) __builtin_bswap64(x)
#endif

#define I32NULLVAL (0x80000000)
#define I64NULLVAL (0x8000000000000000)

// vvvvvvv Trie vvvvvvvvv

typedef struct trie_node trie_node;

struct trie_node {
    trie_node *nodes[256];
};

#define MAX_CHUNKS 20
#define INIT_CHUNK 256

typedef struct {
    void *allocated[MAX_CHUNKS];
    int chunk;
    int nodes_in_chunk;
    trie_node *node_in_chunk, *root;
} trie;

    static void
init_trie(trie *t)
{
    int i;
    t->chunk = 0;
    t->nodes_in_chunk = 0;
    t->node_in_chunk = t->root = NULL;
    for(i=0; i < MAX_CHUNKS; i++)
        t->allocated[i] = NULL;
    return;
}

    static trie_node *
alloc_trie_node(
        trie *t)
{
    trie_node *ret;
    if (!(t->nodes_in_chunk)) {
        if ((t->chunk) >= MAX_CHUNKS) {
            fprintf(stderr, "Too many memory allocations\n");
            exit(EXIT_FAILURE);
        }
        t->nodes_in_chunk = INIT_CHUNK << t->chunk;
        // TODO: check result
        t->allocated[t->chunk] = enif_alloc(t->nodes_in_chunk * sizeof(trie_node));
        t->node_in_chunk = (trie_node *) t->allocated[t->chunk++];
    }
    t->nodes_in_chunk--;
    ret = t->node_in_chunk++;
    memset(ret, 0, sizeof(trie_node));
    return ret;
}

    static void
free_trie_chunks(
        trie *t)
{
    int c;
    for (c = 0; c <= t->chunk; c++)
        enif_free(t->allocated[c]);
    init_trie(t);
}

    static trie_node **
add(
        trie *t,
        int val)
{
    trie_node ** root = &(t->root);
    if ((*root) == NULL)
        (*root) = alloc_trie_node(t);
    root = &((*root)->nodes[val >> 24]);
    if ((*root) == NULL)
        (*root) = alloc_trie_node(t);
    root = &((*root)->nodes[(val & 0x00FF0000) >> 16]);
    if ((*root) == NULL)
        (*root) = alloc_trie_node(t);
    root = &((*root)->nodes[(val & 0x0000FF00) >> 8]);
    if ((*root) == NULL)
        (*root) = alloc_trie_node(t);
    root = &((*root)->nodes[val & 0x000000FF]);
    return root;
}

    static trie_node *
lookup(
        trie *t,
        int val)
{
    trie_node ** root = &(t->root);
    if ((*root) == NULL)
        return NULL;
    root = &((*root)->nodes[val >> 24]);
    if ((*root) == NULL)
        return NULL;
    root = &((*root)->nodes[(val & 0x00FF0000) >> 16]);
    if ((*root) == NULL)
        return NULL;
    root = &((*root)->nodes[(val & 0x0000FF00) >> 8]);
    if ((*root) == NULL)
        return NULL;
    return (*root)->nodes[val & 0x000000FF];
}

    static void
foreach(
        trie *t,
        void (*action) (trie_node *,
            int, void *custom),
        void *custom)
{
    trie_node ** root = &(t->root);
    int c1, c2, c3, c4;
    trie_node *n1, *n2, *n3, *n4;
    int val;
    if ((*root) == NULL)
        return;
    for (c1 = 0; c1 < 256; c1++) {
        n1 = (*root)->nodes[c1];
        if (n1 != NULL) {
            val = c1 << 24;
            for (c2 = 0; c2 < 256; c2++) {
                n2 = n1->nodes[c2];
                if (n2 != NULL) {
                    val = (val & 0xFF000000) | (c2 << 16);
                    for (c3 = 0; c3 < 256; c3++) {
                        n3 = n2->nodes[c3];
                        if (n3 != NULL) {
                            val = (val & 0xFFFF0000) | (c3 << 8);
                            for (c4 = 0; c4 < 256; c4++) {
                                n4 = n3->nodes[c4];
                                val = (val & 0xFFFFFF00) | c4;
                                action(n4, val, custom);
                            }
                        }
                    }
                }
            }
        }
    }
}

    static void
free_trie(
        trie *t,
        void (*free_leaf) (trie_node *))
{
    trie_node ** root = &(t->root);
    int c1, c2, c3, c4;
    trie_node *n1, *n2, *n3, *n4;
    if (!(*root))
        return;
    for (c1 = 0; c1 < 256; c1++) {
        n1 = (*root)->nodes[c1];
        if (n1) {
            for (c2 = 0; c2 < 256; c2++) {
                n2 = n1->nodes[c2];
                if (n2) {
                    for (c3 = 0; c3 < 256; c3++) {
                        n3 = n2->nodes[c3];
                        if (n3) {
                            for (c4 = 0; c4 < 256; c4++) {
                                n4 = n3->nodes[c4];
                                if (free_leaf != NULL)
                                    free_leaf(n4);
                            }
                        }
                    }
                }
            }
        }
    }
    free_trie_chunks(t);
    (*root) = NULL;
}

    static void
sum_values(
        trie_node * n,
        int val,
        void *counter)
{
    (*(intptr_t *)counter) += (intptr_t)n;
}

// ^^^^^^^^ Trie ^^^^^^^^^^

static ErlNifResourceType *srdbe_rw_bin_type = NULL;
static ErlNifResourceType *srdbe_trie_id = NULL;

typedef struct {
    size_t size;
    void *data;
} rw_bin;

static struct {
    ERL_NIF_TERM ok;
    ERL_NIF_TERM not_null;
    ERL_NIF_TERM default_null;
} srdbe_atoms;

    static void
release_rw_bin(ErlNifEnv* env, void* obj)
{
    enif_free(((rw_bin *)obj)->data);
    return;
}

    static void
release_trie_id(ErlNifEnv* env, void* obj)
{
    free_trie((trie *)obj, NULL);
    return;
}

    static int
load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    //puts("Loading nif\r");

    ErlNifResourceType *rw_bin_rt = enif_open_resource_type(env, "srdbe",
            "rw_bin", release_rw_bin, ERL_NIF_RT_CREATE, NULL);
    ErlNifResourceType *trie_id_rt = enif_open_resource_type(env, "srdbe",
            "trie_id", release_trie_id, ERL_NIF_RT_CREATE, NULL);

    if ((rw_bin_rt == NULL) || (trie_id_rt == NULL))
        return -1;

    //puts("Registering resources\r");

    assert(srdbe_rw_bin_type == NULL);
    assert(srdbe_trie_id == NULL);
    srdbe_rw_bin_type = rw_bin_rt;
    srdbe_trie_id = trie_id_rt;

    srdbe_atoms.ok           = enif_make_atom(env, "ok");
    srdbe_atoms.not_null     = enif_make_atom(env, "not_null");
    srdbe_atoms.default_null = enif_make_atom(env, "default_null");

    return 0;
}

    static int
upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    //  puts("Upgrading nif\r");

    ErlNifResourceType *rw_bin_rt = enif_open_resource_type(env, "srdbe",
            "rw_bin", release_rw_bin, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
    ErlNifResourceType *trie_id_rt = enif_open_resource_type(env, "srdbe",
            "trie_id", release_trie_id, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

    if ((rw_bin_rt == NULL) || (trie_id_rt == NULL))
        return -1;

    srdbe_rw_bin_type = rw_bin_rt;
    srdbe_trie_id = trie_id_rt;

    return 0;
}

    static void
unload(ErlNifEnv* env, void* priv_data)
{
    //  puts("Unloading nif\r");
    return;
}

#pragma pack(push,1)

typedef struct {
    char a;
    int16_t b;
} foo;

#pragma pack(pop)

    static ERL_NIF_TERM
test(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_int(env, sizeof(intptr_t));
}

    static ERL_NIF_TERM
lift1_ii(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary index, data, result;

    if(!(
                enif_inspect_binary(env, argv[0], &index) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_alloc_binary(index.size, &result)
        ))
        return enif_make_badarg(env);

    {
        int32_t *indexdata = (int32_t *)index.data;
        int32_t *datadata = (int32_t *)data.data;
        int32_t *resultdata = (int32_t *)result.data;
        unsigned int indexsize = index.size / sizeof(int32_t);
        unsigned int datasize = data.size / sizeof(int32_t);
        unsigned int pos;
        unsigned int i;
        for(i=0; i<indexsize; i++) {
            pos = SWAP32(indexdata[i]);
            if(pos == I32NULLVAL) {
                resultdata[i] = SWAP32(I32NULLVAL);
            }
            else if(!(pos<datasize)) {
                enif_release_binary(&result);
                return enif_make_badarg(env);
            }
            else {
                resultdata[i] = datadata[pos];
            };
        };
    }

    return enif_make_binary(env, &result);
}

    static ERL_NIF_TERM
lift1_is(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary index, data, result;

    if(!(
                enif_inspect_binary(env, argv[0], &index) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_alloc_binary((index.size+31) / 32, &result)
        ))
        return enif_make_badarg(env);

    memset(result.data, 0, result.size);

    {
        int32_t *indexdata = (int32_t *)index.data;
        char *datadata = (char *)data.data;
        char *resultdata = (char *)result.data;
        unsigned int indexsize = index.size / sizeof(int32_t);
        unsigned int datasize = data.size * 8;
        unsigned int pos;
        unsigned int i;
        for(i=0; i<indexsize; i++) {
            pos = SWAP32(indexdata[i]);
            if(pos != I32NULLVAL) {
                if(!(pos<datasize)) {
                    enif_release_binary(&result);
                    return enif_make_badarg(env);
                };
                resultdata[i >> 3] |= (((datadata[pos >> 3] << (pos & 7)) & (1<<7)) >> (i & 7));
            };
        };
    }

    return enif_make_binary(env, &result);
}

    static ERL_NIF_TERM
has_null_i(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary data;
    ERL_NIF_TERM bin, list = argv[0];

    while (!enif_is_empty_list(env, list)) {
        if(!(
                    enif_get_list_cell(env, list, &bin, &list) &&
                    enif_inspect_binary(env, bin, &data)
            ))
            return enif_make_badarg(env);

        {
            int32_t *datadata = (int32_t *)data.data;
            size_t datasize = data.size / sizeof(int32_t);
            size_t i;
            for(i=0; i<datasize; i++)
                if(datadata[i] == SWAP32(I32NULLVAL))
                    return srdbe_atoms.default_null;
        }
    }

    return srdbe_atoms.not_null;
}

    static ERL_NIF_TERM
has_null_d(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary data;
    ERL_NIF_TERM bin, list = argv[0];

    while (!enif_is_empty_list(env, list)) {
        if(!(
                    enif_get_list_cell(env, list, &bin, &list) &&
                    enif_inspect_binary(env, bin, &data)
            ))
            return enif_make_badarg(env);

        {
            int64_t *datadata = (int64_t *)data.data;
            size_t datasize = data.size / sizeof(int64_t);
            size_t i;
            for(i=0; i<datasize; i++)
                if(datadata[i] == SWAP64(I64NULLVAL))
                    return srdbe_atoms.default_null;
        }
    }

    return srdbe_atoms.not_null;
}

    static ERL_NIF_TERM
create_rw_bin_i(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int size;
    rw_bin *bin;
    ERL_NIF_TERM term;

    if(!enif_get_int(env, argv[0], &size))
        return enif_make_badarg(env);

    bin = enif_alloc_resource(srdbe_rw_bin_type, sizeof(rw_bin));
    if(bin == NULL)
        return enif_make_badarg(env);

    bin->size = size * sizeof(int32_t);
    bin->data = enif_alloc(bin->size);

    {
        int32_t *data = (int32_t *)(bin->data);
        int i;
        for(i=0; i<size; i++)
            data[i] = SWAP32(I32NULLVAL);
    }

    term = enif_make_resource(env, bin);
    enif_release_resource(bin);
    return term;
}

    static ERL_NIF_TERM
create_rw_bin_d(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int size;
    rw_bin *bin;
    ERL_NIF_TERM term;

    if(!enif_get_int(env, argv[0], &size))
        return enif_make_badarg(env);

    bin = enif_alloc_resource(srdbe_rw_bin_type, sizeof(rw_bin));
    if(bin == NULL)
        return enif_make_badarg(env);

    bin->size = size * sizeof(int64_t);
    bin->data = enif_alloc(bin->size);

    {
        int64_t *data = (int64_t *)(bin->data);
        int i;
        for(i=0; i<size; i++)
            data[i] = SWAP64(I64NULLVAL);
    }

    term = enif_make_resource(env, bin);
    enif_release_resource(bin);
    return term;
}

    static ERL_NIF_TERM
make_bin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    rw_bin *bin;
    int chunk_size;
    ERL_NIF_TERM list;

    if(!(
                enif_get_resource(env, argv[0], srdbe_rw_bin_type, (void **) &bin) &&
                enif_get_int(env, argv[1], &chunk_size) &&
                (chunk_size > 0)
        ))
        return enif_make_badarg(env);

    {
        size_t size = bin->size;
        char *ptr = (char *)(bin->data) + size;
        size_t reminder = size % chunk_size;

        if(reminder > 0) {
            ptr -= reminder;
            list = enif_make_list1(env,
                    enif_make_resource_binary(env, bin, ptr, reminder));
        }
        else {
            list = enif_make_list(env, 0);
        }

        ptr -= chunk_size;
        while((void *)ptr >= bin->data) {
            list = enif_make_list_cell(
                    env,
                    enif_make_resource_binary(env, bin, ptr, chunk_size),
                    list);
            ptr -= chunk_size;
        }
    }
    return list;
}

    static ERL_NIF_TERM
sum_di(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    rw_bin *bin;
    ErlNifBinary data, index, set;

    if(!(
                enif_get_resource(env, argv[0], srdbe_rw_bin_type, (void **) &bin) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_inspect_binary(env, argv[2], &index) &&
                enif_inspect_binary(env, argv[3], &set) &&
                (data.size == 2*index.size) &&
                (set.size*8 >= index.size/4)
        ))
        return enif_make_badarg(env);

    {
        int64_t *datadata = (int64_t *)data.data;
        int64_t *bindata = (int64_t *)(bin->data);
        int32_t pos, *indexdata = (int32_t *)index.data;
        size_t binsize = (bin->size)/sizeof(int64_t);
        char *setdata = (char *)set.data;
        size_t i, max = data.size/sizeof(int64_t);

        for(i=0; i < max; i++) {
            if(
                    (setdata[i>>3] & ((1<<7) >> (i & 7))) &&
                    (datadata[i] != SWAP64(I64NULLVAL))
              ) {
                pos = SWAP32(indexdata[i]);
                if (!(pos < binsize)) {
                    return enif_make_badarg(env);
                }
                bindata[pos] = (bindata[pos] == SWAP64(I64NULLVAL))
                    ? datadata[i]
                    : SWAP64(SWAP64(bindata[pos])+SWAP64(datadata[i]));
            }
        }
    }

    return srdbe_atoms.ok;
}

    static ERL_NIF_TERM
create_trie_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    trie *t;
    ERL_NIF_TERM term;

    t = enif_alloc_resource(srdbe_trie_id, sizeof(trie));
    if(t == NULL)
        return enif_make_badarg(env);

    init_trie(t);

    term = enif_make_resource(env, t);
    enif_release_resource(t);
    return term;
}

    static ERL_NIF_TERM
projection_make_lu(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    trie *t;
    ErlNifBinary data;
    int index;

    if(!(
                enif_get_resource(env, argv[0], srdbe_trie_id, (void **) &t) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_get_int(env, argv[2], &index)
        ))
        return enif_make_badarg(env);

    {
        int32_t *datadata = (int32_t *)data.data;
        size_t i, datasize = data.size/sizeof(int32_t);
        intptr_t *val;
        for(i=0; i < datasize; i++) {
            val = (intptr_t *)add(t, SWAP32(datadata[i]));
            if(*val) // Lu is expected to be unique
                return enif_make_badarg(env);
            *val = index++;
        }
    }

    return enif_make_int(env, index);
}

    static ERL_NIF_TERM
projection_make_idx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    trie *t;
    ErlNifBinary data, idx;
    int has_null = 0;

    if(!(
                enif_get_resource(env, argv[0], srdbe_trie_id, (void **) &t) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_alloc_binary(data.size, &idx)
        ))
        return enif_make_badarg(env);

    {
        int32_t *datadata = (int32_t *)data.data;
        int32_t *idxdata = (int32_t *)idx.data;
        size_t i, datasize = data.size/sizeof(int32_t);
        intptr_t val;
        for(i=0; i < datasize; i++) {
            val = (intptr_t)lookup(t, SWAP32(datadata[i]));
            if(val)
            {
                idxdata[i] = SWAP32(((int32_t)val) - 1);
            }
            else { // not found
                has_null = 1;
                idxdata[i] = SWAP32(I32NULLVAL);
            }
        }
    }

    return enif_make_tuple2(env,
            has_null ? srdbe_atoms.default_null : srdbe_atoms.not_null,
            enif_make_binary(env, &idx));
}

    static ERL_NIF_TERM
scan_is(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    trie *t;
    ErlNifBinary data, set;

    if(!(
                enif_get_resource(env, argv[0], srdbe_trie_id, (void **) &t) &&
                enif_inspect_binary(env, argv[1], &data) &&
                enif_inspect_binary(env, argv[2], &set) &&
                (set.size*8 >= data.size/4)
        ))
        return enif_make_badarg(env);

    {
        int32_t *datadata = (int32_t *)data.data;
        size_t i, datasize = data.size/sizeof(int32_t);
        char *setdata = (char *)set.data;
        intptr_t *val;
        for(i=0; i<datasize; i++)
            if(setdata[i>>3] & ((1<<7) >> (i & 7)))
            {
                val = (intptr_t *)add(t, SWAP32(datadata[i]));
                *val = 1;
            };
    }

    return srdbe_atoms.ok;
}

typedef struct {
    ErlNifEnv *env;
    ERL_NIF_TERM list;
    ErlNifBinary chunk;
    size_t i;
    int chunk_size;
} gather_state;

static void
gather_i_fun(
        trie_node *node,
        int key,
        void *state)
{
    if(node) {
        gather_state *s=(gather_state *)state;
        ((int32_t *)(s->chunk.data))[s->i++] = SWAP32(key);
        if(s->i >= s->chunk_size)
        {
            if(((s->i)*sizeof(int32_t)) != s->chunk.size)
                enif_realloc_binary(&(s->chunk), (s->i)*sizeof(int32_t));
            s->list = enif_make_list_cell(s->env,
                    enif_make_binary(s->env, &(s->chunk)),
                    s->list);
            enif_alloc_binary(64, &(s->chunk));
            s->i = 0;
        }
        else if(((s->i)*sizeof(int32_t)) >= s->chunk.size)
        {
            enif_realloc_binary(&(s->chunk), (s->chunk.size)*2);
        }
    };
    return;
}

    static ERL_NIF_TERM
gather_i(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    trie *t;
    gather_state state;
    ERL_NIF_TERM result;

    if(!(
                enif_get_resource(env, argv[0], srdbe_trie_id, (void **) &t) &&
                enif_get_int(env, argv[1], &(state.chunk_size)) &&
                enif_alloc_binary(64, &(state.chunk))
        ))
        return enif_make_badarg(env);

    state.env = env;
    state.list = enif_make_list(env, 0);
    state.i = 0;

    foreach(t, gather_i_fun, &state);

    if(state.i)
    {
        enif_realloc_binary(&(state.chunk), (state.i)*sizeof(int32_t));
        state.list = enif_make_list_cell(env,
                    enif_make_binary(env, &(state.chunk)),
                    state.list);
    }
    else
    {
        enif_release_binary(&(state.chunk));
    }

    if(!enif_make_reverse_list(env, state.list, &result))
        return enif_make_badarg(env);

    return result;
}

static ErlNifFunc nif_funcs[] =
{
    {"test", 0, test},
    {"lift1_ii", 2, lift1_ii},
    {"lift1_is", 2, lift1_is},
    {"has_null_i", 1, has_null_i},
    {"has_null_d", 1, has_null_d},
    {"create_rw_bin_i", 1, create_rw_bin_i},
    {"create_rw_bin_d", 1, create_rw_bin_d},
    {"make_bin", 2, make_bin},
    {"sum_di_", 4, sum_di},
    {"create_trie_id", 0, create_trie_id},
    {"projection_make_lu_", 3, projection_make_lu},
    {"projection_make_idx_", 2, projection_make_idx},
    {"scan_is", 3, scan_is},
    {"gather_i", 2, gather_i},
};

ERL_NIF_INIT(srdbe, nif_funcs, load, NULL, upgrade, unload)
