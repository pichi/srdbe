#include <stdio.h>
#include <string.h>
#include <erl_nif.h>

#ifdef USE_NATIVE
#  define SWAP32(x) (x)
#  define SWAP64(x) (x)
#else
#  define SWAP32(x) __builtin_bswap32(x)
#  define SWAP64(x) __builtin_bswap64(x)
#endif

#define I32NULLVAL (0x80000000)

static int 
load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
//  puts("Loading nif\r");
  return 0;
}

static int 
upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
//  puts("Upgrading nif\r");
  return 0;
}

static void
unload(ErlNifEnv* env, void* priv_data)
{
//  puts("Unloading nif\r");
  return;
}

static ERL_NIF_TERM test(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_int(env, sizeof(int));
}

static ERL_NIF_TERM lift1_ii(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary index, data, result;

  if(!enif_inspect_binary(env, argv[0], &index)) {
    return enif_make_badarg(env);
  }

  if(!enif_inspect_binary(env, argv[1], &data)) {
    return enif_make_badarg(env);
  }

  enif_alloc_binary(index.size, &result);

  {
    int32_t *indexdata = (int32_t *)index.data;
    int32_t *datadata = (int32_t *)data.data;
    int32_t *resultdata = (int32_t *)result.data;
    unsigned int indexsize = index.size >> 2;
    unsigned int datasize = data.size >> 2;
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

static ERL_NIF_TERM lift1_is(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary index, data, result;

  if(!enif_inspect_binary(env, argv[0], &index)) {
    return enif_make_badarg(env);
  }

  if(!enif_inspect_binary(env, argv[1], &data)) {
    return enif_make_badarg(env);
  }

  enif_alloc_binary((index.size+31) >> 5, &result);
  memset(result.data, 0, result.size);

  {
    int32_t *indexdata = (int32_t *)index.data;
    char *datadata = (char *)data.data;
    char *resultdata = (char *)result.data;
    unsigned int indexsize = index.size >> 2;
    unsigned int datasize = data.size << 3;
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

static ErlNifFunc nif_funcs[] =
{
    {"test", 0, test},
    {"lift1_ii", 2, lift1_ii},
    {"lift1_is", 2, lift1_is},
};

ERL_NIF_INIT(srdbe, nif_funcs, load, NULL, upgrade, unload)
