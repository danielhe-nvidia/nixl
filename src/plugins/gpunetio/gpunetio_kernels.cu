/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <doca_gpunetio_dev_buf.cuh>
#include <doca_gpunetio_dev_rdma.cuh>
#include <cuda.h>
#include <cuda/atomic>

#include "gpunetio_backend.h"

#define ENABLE_DEBUG 0

__device__ uint32_t
reserve_position (struct docaXferReqGpu *xferReqRing, uint32_t pos) {
    cuda::atomic_ref<uint64_t, cuda::thread_scope_device> index (*xferReqRing[pos].last_rsvd);
    return (index.fetch_add (xferReqRing[pos].num, cuda::std::memory_order_relaxed) & 0xFFFF);
}

__device__ void
wait_post (struct doca_gpu_dev_rdma *rdma_gpu, struct docaXferReqGpu *xferReqRing, uint32_t pos) {
#if ENABLE_DEBUG == 1
    printf ("wait_post waiting xferReqRing[%d].id %d last_posted %ld %p\n",
            pos,
            (xferReqRing[pos].id & DOCA_MAX_COMPLETION_INFLIGHT_MASK),
            *xferReqRing[pos].last_posted,
            (void *)xferReqRing[pos].last_posted);
#endif

    cuda::atomic_ref<uint64_t, cuda::thread_scope_device> index (*xferReqRing[pos].last_posted);
    while (index.load (cuda::std::memory_order_relaxed) !=
           (xferReqRing[pos].id & DOCA_MAX_COMPLETION_INFLIGHT_MASK))
        continue;
    // prevents the compiler from reordering
    asm volatile("fence.acquire.gpu;");
    doca_gpu_dev_rdma_commit_weak (rdma_gpu, 0, xferReqRing[pos].num);
    asm volatile("fence.release.gpu;");

#if ENABLE_DEBUG == 1
    printf ("wait_post updating xferReqRing[%d].id %d last_posted %ld %p to %d\n",
            pos,
            (xferReqRing[pos].id & DOCA_MAX_COMPLETION_INFLIGHT_MASK),
            *xferReqRing[pos].last_posted,
            (void *)xferReqRing[pos].last_posted,
            xferReqRing[pos].id + 1);
#endif
    index.store ((xferReqRing[pos].id + 1) & DOCA_MAX_COMPLETION_INFLIGHT_MASK,
                 cuda::std::memory_order_relaxed);

#if ENABLE_DEBUG == 1
    printf ("wait_post exit xferReqRing[%d].id %d last_posted %ld %p\n",
            pos,
            (xferReqRing[pos].id & DOCA_MAX_COMPLETION_INFLIGHT_MASK),
            *xferReqRing[pos].last_posted,
            (void *)xferReqRing[pos].last_posted);
#endif
}

__global__ void
kernel_read (struct doca_gpu_dev_rdma *rdma_gpu, struct docaXferReqGpu *xferReqRing, uint32_t pos) {
    doca_error_t result;
    struct doca_gpu_buf *lbuf;
    struct doca_gpu_buf *rbuf;
    __shared__ uint32_t base_position;

    // Warmup
    if (xferReqRing == nullptr) return;

    if (threadIdx.x == 0) base_position = reserve_position (xferReqRing, pos);
    __syncthreads();

    if (threadIdx.x < xferReqRing[pos].num) {

        doca_gpu_dev_buf_get_buf (
                (struct doca_gpu_buf_arr *)xferReqRing[pos].larr[threadIdx.x], 0, &lbuf);
        doca_gpu_dev_buf_get_buf (
                (struct doca_gpu_buf_arr *)xferReqRing[pos].rarr[threadIdx.x], 0, &rbuf);

#if ENABLE_DEBUG == 1
        printf (">>>>>>> CUDA rdma read kernel thread %d pos %d descr %d size %d\n",
                threadIdx.x,
                pos,
                (base_position + threadIdx.x) & 0xFFFF,
                (int)xferReqRing[pos].size[threadIdx.x]);
#endif
        result = doca_gpu_dev_rdma_read_weak (rdma_gpu,
                                              0,
                                              rbuf,
                                              0,
                                              lbuf,
                                              0,
                                              xferReqRing[pos].size[threadIdx.x],
                                              xferReqRing[pos].conn_idx,
                                              (base_position + threadIdx.x) & 0xFFFF);
        if (result != DOCA_SUCCESS) printf ("Error %d doca_gpu_dev_rdma_write_strong\n", result);
    }
    __syncthreads();

    if (threadIdx.x == 0) {
        wait_post (rdma_gpu, xferReqRing, pos);

#if ENABLE_DEBUG == 1
        printf (">>>>>>> CUDA rdma read kernel pos %d posted %d buffers\n",
                pos,
                xferReqRing[pos].num);
#endif
        DOCA_GPUNETIO_VOLATILE (xferReqRing[pos].in_use) = 1;
    }
}

__global__ void
kernel_write (struct doca_gpu_dev_rdma *rdma_gpu,
              struct docaXferReqGpu *xferReqRing,
              uint32_t pos) {
    doca_error_t result;
    struct doca_gpu_buf *lbuf;
    struct doca_gpu_buf *rbuf;
    __shared__ uint32_t base_position;

    // Warmup
    if (xferReqRing == nullptr) return;

    if (threadIdx.x == 0) {
        base_position = reserve_position (xferReqRing, pos);
        __threadfence_block();
        // printf("pos %d id %d base %d num %d\n", pos, xferReqRing[pos].id, base_position,
        // xferReqRing[pos].num);
    }
    __syncthreads();

    if (threadIdx.x < xferReqRing[pos].num) {

        doca_gpu_dev_buf_get_buf (
                (struct doca_gpu_buf_arr *)xferReqRing[pos].larr[threadIdx.x], 0, &lbuf);
        doca_gpu_dev_buf_get_buf (
                (struct doca_gpu_buf_arr *)xferReqRing[pos].rarr[threadIdx.x], 0, &rbuf);

#if ENABLE_DEBUG == 1
        printf (">>>>>>> CUDA rdma write kernel thread %d pos %d descr %d size %d\n",
                threadIdx.x,
                pos,
                (base_position + threadIdx.x) & 0xFFFF,
                (int)xferReqRing[pos].size[threadIdx.x]);
#endif
        result = doca_gpu_dev_rdma_write_weak (rdma_gpu,
                                               0,
                                               rbuf,
                                               0,
                                               lbuf,
                                               0,
                                               xferReqRing[pos].size[threadIdx.x],
                                               xferReqRing[pos].conn_idx,
                                               DOCA_GPU_RDMA_WRITE_FLAG_NONE,
                                               (base_position + threadIdx.x) & 0xFFFF);
        if (result != DOCA_SUCCESS) printf ("Error %d doca_gpu_dev_rdma_write_strong\n", result);
    }
    __syncthreads();

    if (threadIdx.x == 0) {
        wait_post (rdma_gpu, xferReqRing, pos);

#if ENABLE_DEBUG == 1
        printf (">>>>>>> CUDA rdma write kernel pos %d posted %d buffers\n",
                pos,
                xferReqRing[pos].num);
#endif
        DOCA_GPUNETIO_VOLATILE (xferReqRing[pos].in_use) = 1;
    }
}

__global__ void
kernel_progress (struct docaXferCompletion *completion_list,
                 struct docaNotifRecv *notif_fill,
                 struct docaNotifRecv *notif_progress,
                 struct docaNotifSend *notif_send_gpu,
                 uint32_t *exit_flag) {
    doca_error_t result;
    uint32_t num_msg = 0, num_msg_notif = 0;
    uint32_t index = 0;
    struct doca_gpu_buf *recv_buf;
    struct doca_gpu_buf *send_buf;
    struct doca_gpu_dev_rdma_r *rdma_gpu_r;

    // Warmup
    if (completion_list == nullptr) return;

    // Wait Xfer & notify
    if (blockIdx.x == 0) {
        while (DOCA_GPUNETIO_VOLATILE (*exit_flag) == 0) {
            // Check xfer completion and send notif
            if (DOCA_GPUNETIO_VOLATILE (completion_list[index].xferReqRingGpu) != nullptr) {
                if (DOCA_GPUNETIO_VOLATILE (completion_list[index].completed) == 0 &&
                    DOCA_GPUNETIO_VOLATILE (completion_list[index].xferReqRingGpu->in_use) == 1) {
                    result = doca_gpu_dev_rdma_wait_all (
                            completion_list[index].xferReqRingGpu->rdma_gpu_data, &num_msg);
                    if (result != DOCA_SUCCESS) {
                        printf ("Error %d doca_gpu_dev_rdma_wait_all xfer\n", result);
                        DOCA_GPUNETIO_VOLATILE (*exit_flag) = 1;
                    }

                    while (num_msg > 0) {
#if ENABLE_DEBUG == 1
                        printf ("Completion %d num recv %d\n", index, num_msg);
#endif

                        if (DOCA_GPUNETIO_VOLATILE (
                                    completion_list[index].xferReqRingGpu->has_notif_msg_idx) !=
                            DOCA_NOTIF_NULL) {
#if ENABLE_DEBUG == 1
                            printf ("Notif after completion at %d id %d sz %d\n",
                                    index,
                                    DOCA_GPUNETIO_VOLATILE (
                                            completion_list[index]
                                                    .xferReqRingGpu->has_notif_msg_idx),
                                    (int)completion_list[index].xferReqRingGpu->msg_sz);
#endif
                            doca_gpu_dev_buf_get_buf (
                                    completion_list[index].xferReqRingGpu->notif_barr_gpu,
                                    completion_list[index].xferReqRingGpu->has_notif_msg_idx,
                                    &send_buf);
                            doca_gpu_dev_rdma_send_strong (
                                    completion_list[index].xferReqRingGpu->rdma_gpu_notif,
                                    0,
                                    send_buf,
                                    0,
                                    completion_list[index].xferReqRingGpu->msg_sz,
                                    0,
                                    DOCA_GPU_RDMA_SEND_FLAG_NONE);

                            result = doca_gpu_dev_rdma_commit_strong (
                                    completion_list[index].xferReqRingGpu->rdma_gpu_notif, 0);
                            if (result != DOCA_SUCCESS)
                                printf ("Error %d doca_gpu_dev_rdma_push\n", result);

                            result = doca_gpu_dev_rdma_wait_all (
                                    completion_list[index].xferReqRingGpu->rdma_gpu_notif,
                                    &num_msg_notif);
                            if (result != DOCA_SUCCESS) {
                                printf ("Error %d doca_gpu_dev_rdma_wait_all notif\n", result);
                                DOCA_GPUNETIO_VOLATILE (*exit_flag) = 1;
                            }
#if ENABLE_DEBUG == 1
                            printf ("Notif correctly sent %d\n", num_msg_notif);
#endif
                        }
                        DOCA_GPUNETIO_VOLATILE (completion_list[index].completed) = 1;
                        num_msg--;
                        index = (index + 1) & DOCA_MAX_COMPLETION_INFLIGHT_MASK;
                    }
                }
            }
        }
    }

    // Receive notif: fill recv in new queue and progress queue
    if (blockIdx.x == 1) {
        while (DOCA_GPUNETIO_VOLATILE (*exit_flag) == 0) {
            // Check received notifications
            if (DOCA_GPUNETIO_VOLATILE (notif_progress->rdma_qp) != nullptr) {
                doca_gpu_dev_rdma_get_recv (notif_progress->rdma_qp, &rdma_gpu_r);
                result = doca_gpu_dev_rdma_recv_wait_all (
                        rdma_gpu_r, DOCA_GPU_RDMA_RECV_WAIT_FLAG_NB, &num_msg, nullptr, nullptr);
                if (result != DOCA_SUCCESS) {
                    printf ("Error %d doca_gpu_dev_rdma_recv_wait_all\n", result);
                    DOCA_GPUNETIO_VOLATILE (*exit_flag) = 1;
                }

#if ENABLE_DEBUG == 1
                if (num_msg > 0) {
                    printf ("kernel received %d notifications\n", num_msg);
                }
#endif

                doca_gpu_dev_rdma_recv_commit_weak (rdma_gpu_r, num_msg);
                DOCA_GPUNETIO_VOLATILE (notif_progress->num_msg) = num_msg;
                asm volatile("fence.release.sys;");
                DOCA_GPUNETIO_VOLATILE (notif_progress->rdma_qp) = nullptr;
            }

            if (DOCA_GPUNETIO_VOLATILE (notif_fill->rdma_qp) != nullptr) {
                result = doca_gpu_dev_rdma_get_recv (notif_fill->rdma_qp, &rdma_gpu_r);
                if (result != DOCA_SUCCESS)
                    printf ("Error %d doca_gpu_dev_rdma_get_recv\n", result);

                for (int idx = 0; idx < DOCA_MAX_NOTIF_INFLIGHT; idx++) {
                    doca_gpu_dev_buf_get_buf (notif_fill->barr_gpu, idx, &recv_buf);
                    result = doca_gpu_dev_rdma_recv_weak (
                            rdma_gpu_r, recv_buf, DOCA_MAX_NOTIF_MESSAGE_SIZE, 0, 0, idx);
                    if (result != DOCA_SUCCESS)
                        printf ("Error %d doca_gpu_dev_rdma_recv_strong\n", result);
                }

                result = doca_gpu_dev_rdma_recv_commit_weak (rdma_gpu_r, DOCA_MAX_NOTIF_INFLIGHT);
                if (result != DOCA_SUCCESS)
                    printf ("Error %d doca_gpu_dev_rdma_recv_commit_strong\n", result);


                DOCA_GPUNETIO_VOLATILE (notif_fill->rdma_qp) = nullptr;
            }
        }
    }

    // Send standalone notifications
    if (blockIdx.x == 2) {
        while (DOCA_GPUNETIO_VOLATILE (*exit_flag) == 0) {
            if (DOCA_GPUNETIO_VOLATILE (notif_send_gpu->rdma_qp) != nullptr) {
#if ENABLE_DEBUG == 1
                printf ("Notif standalone id %d\n",
                        DOCA_GPUNETIO_VOLATILE (notif_send_gpu->buf_idx));
#endif

                doca_gpu_dev_buf_get_buf (DOCA_GPUNETIO_VOLATILE (notif_send_gpu->barr_gpu),
                                          DOCA_GPUNETIO_VOLATILE (notif_send_gpu->buf_idx),
                                          &send_buf);
                result = doca_gpu_dev_rdma_send_strong (notif_send_gpu->rdma_qp,
                                                        0,
                                                        send_buf,
                                                        0,
                                                        notif_send_gpu->msg_sz,
                                                        0,
                                                        DOCA_GPU_RDMA_SEND_FLAG_NONE);
                if (result != DOCA_SUCCESS)
                    printf ("Error %d doca_gpu_dev_rdma_send_strong\n", result);

                result = doca_gpu_dev_rdma_commit_strong (notif_send_gpu->rdma_qp, 0);
                if (result != DOCA_SUCCESS) printf ("Error %d doca_gpu_dev_rdma_push\n", result);

                result = doca_gpu_dev_rdma_wait_all (notif_send_gpu->rdma_qp, &num_msg_notif);
                if (result != DOCA_SUCCESS) {
                    printf ("Error %d doca_gpu_dev_rdma_wait_all standalone\n", result);
                    DOCA_GPUNETIO_VOLATILE (*exit_flag) = 1;
                }
#if ENABLE_DEBUG == 1
                printf ("Notif correctly sent %d\n", num_msg_notif);
#endif

                DOCA_GPUNETIO_VOLATILE (notif_send_gpu->rdma_qp) = nullptr;
            }
        }
    }
}

doca_error_t
doca_kernel_write (cudaStream_t stream,
                   struct doca_gpu_dev_rdma *rdma_gpu,
                   struct docaXferReqGpu *xferReqRing,
                   uint32_t pos) {
    cudaError_t result = cudaSuccess;

    /* Check no previous CUDA errors */
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    kernel_write<<<1, DOCA_XFER_REQ_SIZE, 0, stream>>> (rdma_gpu, xferReqRing, pos);
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    return DOCA_SUCCESS;
}

doca_error_t
doca_kernel_read (cudaStream_t stream,
                  struct doca_gpu_dev_rdma *rdma_gpu,
                  struct docaXferReqGpu *xferReqRing,
                  uint32_t pos) {
    cudaError_t result = cudaSuccess;

    /* Check no previous CUDA errors */
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    kernel_read<<<1, DOCA_XFER_REQ_SIZE, 0, stream>>> (rdma_gpu, xferReqRing, pos);
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    return DOCA_SUCCESS;
}

doca_error_t
doca_kernel_progress (cudaStream_t stream,
                      struct docaXferCompletion *completion_list,
                      struct docaNotifRecv *notif_fill,
                      struct docaNotifRecv *notif_progress,
                      struct docaNotifSend *notif_send_gpu,
                      uint32_t *exit_flag) {
    cudaError_t result = cudaSuccess;

    /* Check no previous CUDA errors */
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    kernel_progress<<<3, 1, 0, stream>>> (
            completion_list, notif_fill, notif_progress, notif_send_gpu, exit_flag);
    result = cudaGetLastError();
    if (result != cudaSuccess) {
        fprintf (stderr,
                 "[%s:%d] cuda failed with %s",
                 __FILE__,
                 __LINE__,
                 cudaGetErrorString (result));
        return DOCA_ERROR_BAD_STATE;
    }

    return DOCA_SUCCESS;
}
