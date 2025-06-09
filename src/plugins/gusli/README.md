# NIXL Gusli Plugin

This plugin utilizes `gusli_clnt.so` as an I/O backend for NIXL.

## Usage
1. Build and install [Gusli](https://GUSLITODO). make clean all
2. Ensure that libraries: `libgusli_clnt.so`, are installed under `/usr/lib/`.
3. Ensure that headers are installed under `/usr/include/gusli_*.hpp`.
4. Build NIXL.
5. Once the Gusli Backend is built, you can use it in your data transfer task by specifying the backend name as "GUSLI":

```cpp
nixl_status_t ret1;
std::string ret_s1;
nixlAgentConfig cfg(true);
nixl_b_params_t init1;
nixl_mem_list_t mems1;
nixlBackendH      *gusli;
nixlAgent A1(agent1, cfg);
ret1 = A1.getPluginParams("GUSLI", mems1, init1);
assert (ret1 == NIXL_SUCCESS);
ret1 = A1.createBackend("GUSLI", init1, gusli);
...
```

