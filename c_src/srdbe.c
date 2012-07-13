#include <stdio.h>
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

static ErlNifResourceType *srdbe_rw_bin_type = NULL;

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

static int 
load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
  //  puts("Loading nif\r");

  ErlNifResourceType *rt = enif_open_resource_type(env, "srdbe",
      "rw_bin", release_rw_bin, ERL_NIF_RT_CREATE, NULL);

  if (rt == NULL)
    return -1;

  assert(srdbe_rw_bin_type == NULL);
  srdbe_rw_bin_type = rt;

  srdbe_atoms.ok           = enif_make_atom(env, "ok");
  srdbe_atoms.not_null     = enif_make_atom(env, "not_null");
  srdbe_atoms.default_null = enif_make_atom(env, "default_null");

  return 0;
}

  static int
upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
  //  puts("Upgrading nif\r");

  ErlNifResourceType *rt = enif_open_resource_type(env, "srdbe",
      "rw_bin", release_rw_bin, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

  if (rt == NULL)
    return -1;

  srdbe_rw_bin_type = rt;

  return 0;
}

  static void
unload(ErlNifEnv* env, void* priv_data)
{
  //  puts("Unloading nif\r");
  return;
}

  static ERL_NIF_TERM
test(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  return enif_make_int(env, sizeof(size_t));
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
};

ERL_NIF_INIT(srdbe, nif_funcs, load, NULL, upgrade, unload)
