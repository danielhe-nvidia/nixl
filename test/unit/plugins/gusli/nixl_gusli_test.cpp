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
#include <filesystem>
#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iomanip>
#include <cassert>
#include <cstring>
#include <string>
#include <absl/strings/str_format.h>
#include "nixl.h"
#include "nixl_params.h"
#include "nixl_descriptors.h"
#include "common/nixl_time.h"
#include <stdexcept>
#include <cstdio>
#include <getopt.h>

#ifndef __stringify
	#define __stringify_1(x...)	#x
	#define __stringify(x...)	__stringify_1(x)
#endif

namespace {
	std::ostream& err_log = std::cerr;
	std::ostream& out_log = std::cerr; //std::cout; GUSLITODO
};
class gtest {		// Gusli tester class
	static constexpr const size_t gb_size = (1 << 30);
	static constexpr const int line_width = 60;
	static constexpr const char* agent_name = "GUSLITester";
	const char* line_str = "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
 private:
	int num_transfers;
	size_t transfer_size;
	long page_size;
	long sg_buf_size;
	void* ptr;					// RAM buffer for ios

	static std::string center_str(const std::string& str) { return std::string((line_width - str.length()) / 2, ' ') + str; }

	bool test_pattern_do(void* buffer, size_t size, const char* action) {
		static constexpr const size_t test_phrase_len = 32;
		char test_phrase[test_phrase_len+1] __attribute__((aligned(sizeof(long))));
		strcpy(test_phrase, "|NIXL bdev 32[b] GUSLI pattern |");
		char* buf = (char*)buffer;
		if (action[0] == 'f') {						// Fill
			for (size_t i = 0; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				memcpy(&buf[i], test_phrase, test_phrase_len);
			}
		} else if (action[0] == 'c') {				// Clear
			memset(buffer, 'c', size);
		} else if (action[0] == 'p') {				// Print
			out_log << "BUF: "; out_log.write((char*)buffer, 16); out_log << "\n";
		} else {									// Verify
			for (size_t i = 0; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				if (0 != memcmp(&buf[i], test_phrase, test_phrase_len)) {
					err_log << "DRAM buffer " << i << " validation failed with error\n";
					return false;
				}
			}
		}
		return true;
	}

	static std::string format_duration(nixlTime::us_t us) {	// Helper function to format duration
		const nixlTime::ms_t ms = us/1000.0;
		if (ms < 1000)
			return absl::StrFormat("%.0f[ms]", ms);
		const double seconds = ms / 1000.0;
		return absl::StrFormat("%.3f[sec]", seconds);
	}

	void printProgress(float progress) {
		static constexpr const int progress_bar_width = (line_width - 2); // -2 for the brackets
		out_log << "[";
		const int pos = (progress_bar_width * progress);
		for (int i = 0; i < progress_bar_width; ++i) {
			if (i < pos) out_log << "=";
			else if (i == pos) out_log << ">";
			else out_log << " ";
		}
		out_log << absl::StrFormat("] %.1f%% ", progress * 100.0);
		if (progress >= 1.0) {	// Add completion indicator
			out_log << "DONE!" << std::endl;
		} else {
			out_log << "\r";
			out_log.flush();
		}
	}
	std::string phase_title(const std::string& title) {
		static int phase_num = 1;
		return absl::StrFormat("PHASE %d: %s", phase_num++, title.c_str());
	}

	void print_segment_title(const std::string& title) {
		out_log << line_str << center_str(title) << line_str;
	}

 public:
	gtest(int _num_transfers, size_t _transfer_size) : num_transfers(_num_transfers), transfer_size(_transfer_size), page_size(-1), ptr(nullptr) {
		page_size = sysconf(_SC_PAGESIZE);
		if (page_size <= 0) {
			err_log << "Error: Invalid page size returned by sysconf" << std::endl;
			return;
		}
		if (num_transfers % 4)					// Make num_transfers aligned to 4
			num_transfers = ((num_transfers + 3) / 4) * 4;
		if ((transfer_size % page_size) != 0) 	// align transfer size to page size
			transfer_size = ((transfer_size + page_size - 1) / page_size) * page_size;
		sg_buf_size = num_transfers*32;		// SG (scatter gather element is 24[b]: ptr+len+offet. Round to 32)
		sg_buf_size = ((sg_buf_size + page_size - 1) / page_size) * page_size;	// Round to page size
	}
	~gtest() {
		if (ptr) free(ptr);
	}

	int run_write_read_verify(void) {
		nixlAgent agent(agent_name, nixlAgentConfig(true));

		// Set up backend parameters for gusli::global_clnt_context::init_params
		#define UUID_LOCAL_FILE 11
		#define UUID_NVME__DISK 17
		nixl_b_params_t params;
		params["client_name"] = agent_name;
		params["config_file"] = "# version=1, bdevs: UUID-16b, type, attach_op, direct, path, security_cookie\n"
			__stringify(UUID_LOCAL_FILE) " f W N ./store.bin  sec=0x3\n"		// Local file in non direct mode
			__stringify(UUID_NVME__DISK) " K X D /dev/nvme0n1 sec=0x7\n";	// NVME in direct mode
		params["max_num_simultaneous_requests"] = std::to_string(256); // Not std::to_string(num_transfers); GUSLITODO

		// Print test configuration information
		const size_t n_total_mapped_bytes = num_transfers * transfer_size;
		print_segment_title("NIXL STORAGE TEST STARTING (GUSLI PLUGIN)");
		out_log << absl::StrFormat("Configuration:\n");
		out_log << absl::StrFormat("- Number of transfers=%d\n", num_transfers);
		out_log << absl::StrFormat("- Transfer=%zu[KB], sg=%zu[KB]\n", (transfer_size >> 10), (sg_buf_size >> 10));
		out_log << absl::StrFormat("- Total data: %.2f[GB]\n", float(n_total_mapped_bytes) / gb_size);
		out_log << absl::StrFormat("- Backend: %s\n", "GUSLI");
		out_log << absl::StrFormat("- Direct I/O: %s\n", "enabled");
		out_log << line_str;

		// Create GUSLI backend first - before allocating any resources
		nixlBackendH* n_backend = nullptr;
		{
			const nixl_status_t status = agent.createBackend("GUSLI", params, n_backend);
			if (status != NIXL_SUCCESS) {
				err_log << line_str << center_str("ERROR: GUSLI Backend Creation Failed: " + nixlEnumStrings::statusStr(status)) << line_str;
				return 1;
			}
		}

		const int bdev_descriptor = UUID_LOCAL_FILE;
		print_segment_title(phase_title("Allocating buffers, bdev " + UUID_LOCAL_FILE));
		if (posix_memalign(&ptr, page_size, n_total_mapped_bytes + sg_buf_size) != 0) {
			err_log << "DRAM allocation failed" << std::endl;
			return 1;
		}
		nixl_xfer_dlist_t bdev_io_src(DRAM_SEG), bdev_io_dst(BLK_SEG);
		const size_t bdev_byte_offset = (1UL << 20);	// Write at offset 1[MB] in block device
		{
			nixlBlobDesc d;
			d.devId = bdev_descriptor;
			// First entry is dummy, with enough space for scatter gather
			d.len = sg_buf_size;
			d.addr = (uintptr_t)((u_int64_t)ptr + n_total_mapped_bytes);
			bdev_io_src.addDesc(d);
			d.addr = bdev_byte_offset;	// Dummy
			bdev_io_dst.addDesc(d);

			for (int i = 0; i < num_transfers; ++i) {
				const size_t offset = (i*transfer_size);
				d.len = transfer_size;
				d.addr = (uintptr_t)((size_t)ptr + offset);	// Offset in RAM buffer
				bdev_io_src.addDesc(d);
				d.addr = bdev_byte_offset + offset;
				bdev_io_dst.addDesc(d);
				printProgress(float(i + 1) / num_transfers);
			}
		}

		print_segment_title(phase_title("Registering memory with NIXL"));
		nixl_reg_dlist_t dram_reg(DRAM_SEG), bdev_reg(BLK_SEG);
		{	// Register the large IO buffer + additional sg
			nixlBlobDesc d;
			d.devId = bdev_descriptor;
			d.len = n_total_mapped_bytes + sg_buf_size;
			d.addr = (uintptr_t)ptr; dram_reg.addDesc(d);

			// Just for debug, register in 4 quarters, can register as 1 buffer as well
			d.len = n_total_mapped_bytes/4;	// Security reason: Enforces all IO's within registerd memory. Not needed internally by the plugin
			for (int i = 0; i < 4; i++ ) {
				d.addr = bdev_byte_offset + i*d.len; bdev_reg.addDesc(d);
			}

			enum nixl_status_t rv;
			rv = agent.registerMem(dram_reg);
			if (rv != NIXL_SUCCESS) {
				err_log << "Failed reg:" << nixlEnumStrings::memTypeStr(dram_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(0.5f);
			rv = agent.registerMem(bdev_reg);
			if (rv != NIXL_SUCCESS) {
				err_log << "Failed reg:" << nixlEnumStrings::memTypeStr(bdev_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(1.0f);
		}

		#define LOG_IO_ERR_RETURN(msg, status) { if (status < NIXL_SUCCESS) { err_log << msg << nixlEnumStrings::statusStr(status) << std::endl; if (treq) agent.releaseXferReq(treq); return -1; } } while (0)
		enum nixl_xfer_op_t io_phases[] = {NIXL_WRITE, NIXL_READ};	// First write - then read
		print_segment_title(phase_title("1 xfer element Write-Read-Verify"));
		{
			nixl_xfer_dlist_t bdev_io_1src(DRAM_SEG), bdev_io_1dst(BLK_SEG);
			bdev_io_1src.addDesc(bdev_io_src[4]);
			bdev_io_1dst.addDesc(bdev_io_dst[4]);
			test_pattern_do(ptr, transfer_size, "fill");
			for (int i = 0; i < 2; i++ ) {
				const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
				nixlXferReqH* treq = nullptr;
				//err_log << "PrepXfer: " << agent_name << ", RAM=" << (void*)bdev_io_1src[0].addr << ",len=" << bdev_io_1src[0].len << ", DISK=" << (void*)bdev_io_1dst[0].addr << ",len=" << bdev_io_1dst[0].len << "\n";
				nixl_status_t status = agent.createXferReq(io_phases[i], bdev_io_1src, bdev_io_1dst, agent_name, treq);
				LOG_IO_ERR_RETURN("Failed to create req, rv=", status);
				status = agent.postXferReq(treq);
				LOG_IO_ERR_RETURN("Failed to post req, rv=", status);
				do { // Wait for transfer to complete
					status = agent.getXferStatus(treq);
					LOG_IO_ERR_RETURN("Failed during transfer req, rv=", status);
				} while (status == NIXL_IN_PROG);
				if (io_phases[i] == NIXL_WRITE) {		// Clear buffers before read
					test_pattern_do(ptr, transfer_size, "clear");
				}
				agent.releaseXferReq(treq);
			}
			test_pattern_do(ptr, transfer_size, "verify");
		}

		nixlTime::us_t total_time(0);
		double total_data_gb(0);
		test_pattern_do(ptr, n_total_mapped_bytes, "fill");
		for (int i = 0; i < 2; i++ ) {
			const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
			print_segment_title(phase_title(absl::StrFormat("%s Test, nIOs=%u", io_t_str.c_str(), (int)bdev_io_src.descCount()-1)));
			nixlXferReqH* treq = nullptr;
			nixl_status_t status = agent.createXferReq(io_phases[i], bdev_io_src, bdev_io_dst, agent_name, treq);
			LOG_IO_ERR_RETURN("Failed to create req, rv=", status);
			const nixlTime::us_t time_start = nixlTime::getUs();
			status = agent.postXferReq(treq);
			LOG_IO_ERR_RETURN("Failed to post req, rv=", status);
			do { // Wait for transfer to complete
				status = agent.getXferStatus(treq);
				LOG_IO_ERR_RETURN("Failed during transfer req, rv=", status);
			} while (status == NIXL_IN_PROG);

			const nixlTime::us_t time_end = nixlTime::getUs();
			const nixlTime::us_t time_duration = time_end - time_start;
			const double data_gb = (double)n_total_mapped_bytes / (double)gb_size;
			const double seconds = (time_duration / 1000000.0);
			out_log << io_t_str << " completed with rv=" << nixlEnumStrings::statusStr(status) << std::endl;
			out_log << "- Time: " << format_duration(time_duration) << std::endl;
			out_log << "- Data: " << std::fixed << std::setprecision(2) << data_gb << "[GB]" << std::endl;
			out_log << "- Speed: " << (data_gb / seconds) << "[GB/s]" << std::endl;
			total_time += time_duration;
			total_data_gb += data_gb;
			agent.releaseXferReq(treq);
			if (io_phases[i] == NIXL_WRITE) {		// Clear buffers before read
				print_segment_title(phase_title("Clearing DRAM buffers"));
				test_pattern_do(ptr, n_total_mapped_bytes, "clear");
			}
		}
		print_segment_title(phase_title("Validating read data"));
		test_pattern_do(ptr, n_total_mapped_bytes, "verify");

		{	print_segment_title(phase_title("Un-Registering memory with NIXL"));
			nixl_status_t dreg = agent.deregisterMem(bdev_reg);
			err_log << "Failed de-reg:" << nixlEnumStrings::memTypeStr(bdev_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(dreg) << std::endl;
			dreg = agent.deregisterMem(dram_reg);
			err_log << "Failed de-reg:" << nixlEnumStrings::memTypeStr(dram_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(dreg) << std::endl;
		}

		print_segment_title("TEST SUMMARY");
		out_log << "Total time: " << format_duration(total_time) << std::endl;
		out_log << "Total data: " << std::fixed << std::setprecision(2) << total_data_gb << "[GB]" << line_str;
		return 0;
	}
};

int main(int argc, char *argv[]) {
	out_log << "NIXL GUSLI Plugin Test" << std::endl;
	static constexpr const int default_num_transfers = 1024;
	static constexpr const size_t default_transfer_size = 1 * 512 * 1024; // 512KB
	int opt, num_transfers = default_num_transfers;
	size_t transfer_size = default_transfer_size;

	while ((opt = getopt(argc, argv, "n:s:d:h")) != -1) {
		switch (opt) {
			case 'n': num_transfers = std::stoi( optarg); break;
			case 's': transfer_size = std::stoull(optarg); break;
			case 'h':
			default:
				out_log << absl::StrFormat("Usage: %s [-n num_transfers] [-s transfer_size] [-h]", argv[0]) << std::endl;
				out_log << absl::StrFormat("  -n num_transfers      Number of transfers (default: %d)", default_num_transfers) << std::endl;
				out_log << absl::StrFormat("  -s transfer_size      Size of each transfer in bytes (default: %zu)", default_transfer_size) << std::endl;
				out_log << absl::StrFormat("  -h                    Show this help message") << std::endl;
				return (opt == 'h') ? 0 : 1;
		}
	}

	gtest base_test(num_transfers, transfer_size);
	if (base_test.run_write_read_verify() < 0)
		return -1;
	return 0;
}