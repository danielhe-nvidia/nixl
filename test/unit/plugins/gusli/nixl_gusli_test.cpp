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

namespace {
    const size_t page_size = sysconf(_SC_PAGESIZE);

	static constexpr const int default_num_transfers = 1024;
	static constexpr const size_t default_transfer_size = 1 * 512 * 1024; // 512KB
	static constexpr const size_t kb_size = (1 << 10), mb_size = (1 << 20), gb_size = (1 << 30);
	static constexpr const int line_width = 60;
	static constexpr const std::string line_str(line_width, '=');
	static constexpr const char default_test_files_dir_path[] = "tmp/testfiles";
	static constexpr const char* agent_name = "GUSLITester";

	constexpr double us_to_s(double us) { return us / 1000000.0; }
	std::string center_str(const std::string& str) { return std::string((line_width - str.length()) / 2, ' ') + str; }

	struct PosixMemalignDeleter {
		void operator()(void* ptr) const {
			if (ptr) free(ptr);
		}
	};

	void fill_test_pattern(void* buffer, size_t size) {	// Helper function to fill buffer with repeating pattern
		static constexpr const char test_phrase[] = "|NIXL Storage Test Pattern 2025 GUSLI";
		static constexpr const size_t test_phrase_len = sizeof(test_phrase) - 1; // -1 to exclude null terminator
		char* buf = (char*)buffer;
		size_t i = 0;
		while (i < size) {
			const size_t remaining = (size - i);
			const size_t copy_len = (remaining < test_phrase_len) ? remaining : test_phrase_len;
			memcpy(&buf[i], test_phrase, copy_len);
			i += copy_len;
		}
	}
	void clear_buffer(void* buffer, size_t size) { memset(buffer, 0, size); }

	std::string format_duration(nixlTime::us_t us) {	// Helper function to format duration
		const nixlTime::ms_t ms = us/1000.0;
		if (ms < 1000)
			return absl::StrFormat("%.0f[ms]", ms);
		const double seconds = ms / 1000.0;
		return absl::StrFormat("%.3f[sec]", seconds);
	}

	std::string generate_timestamped_filename(void) { // Helper function to generate timestamped filename
		const std::time_t t = std::time(nullptr);
		char timestamp[100];
		std::strftime(timestamp, sizeof(timestamp), "%Y%m%d%H%M%S", std::localtime(&t));
		return std::string("testfile.txt") + std::string(timestamp);
	}

	void printProgress(float progress) {
		static constexpr const int progress_bar_width = (line_width - 2); // -2 for the brackets
		std::cout << "[";
		const int pos = (progress_bar_width * progress);
		for (int i = 0; i < progress_bar_width; ++i) {
			if (i < pos) std::cout << "=";
			else if (i == pos) std::cout << ">";
			else std::cout << " ";
		}
		std::cout << absl::StrFormat("] %.1f%% ", progress * 100.0);
		if (progress >= 1.0) {	// Add completion indicator
			std::cout << "DONE!" << std::endl;
		} else {
			std::cout << "\r";
			std::cout.flush();
		}
	}
	std::string phase_title(const std::string& title) {
		static int phase_num = 1;
		return absl::StrFormat("PHASE %d: %s", phase_num++, title);
	}

	void print_segment_title(const std::string& title) {
		std::cout << std::endl << line_str << std::endl;
		std::cout << center_str(title) << std::endl;
		std::cout << line_str << std::endl;
	}

	class tempFile {
	 public:
		int fd;
		std::string path;
		tempFile(const std::string& filename, int flags, mode_t mode = 0600) : path(filename) {
			fd = open(filename.c_str(), flags, mode);	// Constructor: opens the file and stores the fd and path
			if (fd == -1) {
				throw std::runtime_error("Failed to open file: " + filename);
			}
		}
		tempFile(const tempFile&) = delete;		// Deleted copy constructor and assignment to avoid double-close/unlink
		tempFile& operator=(const tempFile&) = delete;
		tempFile(tempFile&& other) noexcept : fd(other.fd), path(std::move(other.path)) { other.fd = -1; }	// Move constructor and assignment
		tempFile& operator=(tempFile&& other) noexcept {
			if (this != &other) {
				close_fd();
				path = std::move(other.path);
				fd = other.fd;
				other.fd = -1;
			}
			return *this;
		}
		operator int() const { return fd; }	// Conversion operator to int (file descriptor)

		~tempFile() {	// Destructor: closes the fd and deletes the file
			close_fd();
			if (!path.empty())
				unlink(path.c_str());
		}
	 private:
		void close_fd(void) {
			if (fd != -1) {
				close(fd);
				fd = -1;
			}
		}
	};
}

int main(int argc, char *argv[]) {
	std::cout << "NIXL GUSLI Plugin Test" << std::endl;
	int                opt;
	int                num_transfers = default_num_transfers;
	size_t             transfer_size = default_transfer_size;
	std::string        test_files_dir_path = default_test_files_dir_path;
	long               page_size = sysconf(_SC_PAGESIZE);

    while ((opt = getopt(argc, argv, "n:s:d:DUh")) != -1) {
        switch (opt) {
            case 'n': num_transfers = std::stoi(optarg); break;
            case 's': transfer_size = std::stoull(optarg); break;
            case 'd': test_files_dir_path = optarg; break;
            case 'h':
            default:
                std::cout << absl::StrFormat("Usage: %s [-n num_transfers] [-s transfer_size] [-d test_files_dir_path] [-D] [-U]", argv[0]) << std::endl;
                std::cout << absl::StrFormat("  -n num_transfers      Number of transfers (default: %d)", default_num_transfers) << std::endl;
                std::cout << absl::StrFormat("  -s transfer_size      Size of each transfer in bytes (default: %zu)", default_transfer_size) << std::endl;
                std::cout << absl::StrFormat("  -d test_files_dir_path Directory for test files, strongly recommended to use nvme device (default: %s)", default_test_files_dir_path) << std::endl;
                std::cout << absl::StrFormat("  -D                    Use O_DIRECT for file I/O") << std::endl;
                std::cout << absl::StrFormat("  -h                    Show this help message") << std::endl;
                return (opt == 'h') ? 0 : 1;
        }
    }

	if (page_size <= 0) {
		std::cerr << "Error: Invalid page size returned by sysconf" << std::endl;
		return 1;
	}
	if (transfer_size % page_size != 0) {		// align transfer size to page size
		transfer_size = ((transfer_size + page_size - 1) / page_size) * page_size;
		std::cout << "Adjusted transfer size to " << transfer_size << "[bytes]" << std::endl;
	}

	// Convert directory path to absolute path using std::filesystem
	std::filesystem::path path_obj(test_files_dir_path);
	std::filesystem::create_directories(path_obj);
	std::string abs_path = std::filesystem::absolute(path_obj).string();

	// Initialize NIXL components first
	nixlAgent agent(agent_name, nixlAgentConfig(true));

	// Set up backend parameters for gusli::global_clnt_context::init_params
	nixl_b_params_t params;
	params["log"] = "????";		// GUSLITODO
	params["client_name"] = agent_name;
	params["config_file"] = "# version=1, bdevs: UUID, type, attach_op, direct, path, security_cookie\n"
		"050e8400 f W N ./store.bin  sec=0x3\n"		// Local file in non direct mode
		"2b3f28dc K X D /dev/nvme0n1 sec=0x7\n";	// NVME in direct mode
	params["max_num_simultaneous_requests"] = std::to_string(num_transfers);

	// Print test configuration information
	print_segment_title("NIXL STORAGE TEST STARTING (GUSLI PLUGIN)");
	std::cout << absl::StrFormat("Configuration:\n");
	std::cout << absl::StrFormat("- Number of transfers: %d\n", num_transfers);
	std::cout << absl::StrFormat("- Transfer size: %zu[B]\n", transfer_size);
	std::cout << absl::StrFormat("- Total data: %.2f[GB]\n", (float(transfer_size) * num_transfers) / gb_size);
	std::cout << absl::StrFormat("- Directory: %s\n", abs_path);
	std::cout << absl::StrFormat("- Backend: %s\n", "GUSLI");
	std::cout << absl::StrFormat("- Direct I/O: %s\n", "enabled");
	std::cout << std::endl << line_str << std::endl;

	// Create GUSLI backend first - before allocating any resources
	nixlBackendH* n_backend = nullptr;
	const nixl_status_t status = agent.createBackend("GUSLI", params, n_backend);
	if (status != NIXL_SUCCESS) {
		std::cerr << std::endl << line_str << std::endl << center_str("ERROR: Backend Creation Failed") << std::endl << line_str << std::endl;
		std::cerr << "Error creating GUSLI backend: " << nixlEnumStrings::statusStr(status) << std::endl;
		std::cerr << std::endl << line_str << std::endl;
		return 1;
	}

    try {
        print_segment_title(phase_title("Allocating and initializing buffers"));

		// Allocate resources
		std::vector<std::unique_ptr<void, PosixMemalignDeleter>> dram_addr;
		dram_addr.reserve(num_transfers);

        std::vector<tempFile> fd;
        fd.reserve(num_transfers);

        // File open flags
        int file_open_flags = O_RDWR|O_CREAT;
        if (use_direct_io) {
            file_open_flags |= O_DIRECT;
        }
        mode_t file_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;  // rw-r--r--

        // Create descriptor lists
        nixl_reg_dlist_t dram_for_posix(DRAM_SEG);
        nixl_reg_dlist_t file_for_posix(FILE_SEG);
        nixl_xfer_dlist_t dram_for_posix_xfer(DRAM_SEG);
        nixl_xfer_dlist_t file_for_posix_xfer(FILE_SEG);
        std::unique_ptr<nixlBlobDesc[]> dram_buf(new nixlBlobDesc[num_transfers]);
        std::unique_ptr<nixlBlobDesc[]> ftrans(new nixlBlobDesc[num_transfers]);
        nixlXferReqH* treq = nullptr;
        std::string name;

        // Control variables
        int ret = 0;
        int i = 0;
        nixlTime::us_t time_start;
        nixlTime::us_t time_end;
        nixlTime::us_t time_duration;
        nixlTime::us_t total_time(0);
        double total_data_gb(0);
        double gbps;
        double seconds;
        double data_gb;

        // Allocate and initialize DRAM buffer
        for (i = 0; i < num_transfers; ++i) {
            void* ptr;
            if (posix_memalign(&ptr, page_size, transfer_size) != 0) {
                std::cerr << "DRAM allocation failed" << std::endl;
                return 1;
            }
            dram_addr.emplace_back(ptr);
            fill_test_pattern(dram_addr.back().get(), transfer_size);

            // Create test file
            name = generate_timestamped_filename();
            name = test_files_dir_path + "/" + name + "_" + std::to_string(i);

            try {
                fd.emplace_back(name, file_open_flags, file_mode);
            } catch (const std::exception& e) {
                std::cerr << "Failed to open file: " << name << " - " << e.what() << std::endl;
                return 1;
            }

            dram_buf[i].addr   = (uintptr_t)(dram_addr.back().get());
            dram_buf[i].len    = transfer_size;
            dram_buf[i].devId  = 0;
            dram_for_posix.addDesc(dram_buf[i]);
            dram_for_posix_xfer.addDesc(dram_buf[i]);

            ftrans[i].addr  = 0;
            ftrans[i].len   = transfer_size;
            ftrans[i].devId = fd[i];
            file_for_posix.addDesc(ftrans[i]);
            file_for_posix_xfer.addDesc(ftrans[i]);

            printProgress(float(i + 1) / num_transfers);
        }

        print_segment_title(phase_title("Registering memory with NIXL"));

        i = 0;
        ret = agent.registerMem(dram_for_posix);
        if (ret != NIXL_SUCCESS) {
            std::cerr << "Failed to register DRAM memory with NIXL" << std::endl;
            return 1;
        }
        printProgress(float(++i) / 2);

        ret = agent.registerMem(file_for_posix);
        if (ret != NIXL_SUCCESS) {
            std::cerr << "Failed to register file memory with NIXL" << std::endl;
            return 1;
        }
        printProgress(float(i + 1) / 2);

        print_segment_title(phase_title("Memory to File Transfer (Write Test)"));

        status = agent.createXferReq(NIXL_WRITE, dram_for_posix_xfer, file_for_posix_xfer, agent_name, treq);
        if (status != NIXL_SUCCESS) {
            std::cerr << "Failed to create write transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
            return 1;
        }

        time_start = nixlTime::getUs();
        status = agent.postXferReq(treq);
        if (status < 0) {
            std::cerr << "Failed to post write transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
            agent.releaseXferReq(treq);
            return 1;
        }

        // Wait for transfer to complete
        do {
            status = agent.getXferStatus(treq);
            if (status < 0) {
                std::cerr << "Error during write transfer - status: " << nixlEnumStrings::statusStr(status) << std::endl;
                agent.releaseXferReq(treq);
                return 1;
            }
        } while (status == NIXL_IN_PROG);

        time_end = nixlTime::getUs();
        time_duration = time_end - time_start;
        total_time += time_duration;

        data_gb = (float(transfer_size) * num_transfers) / (gb_size);
        total_data_gb += data_gb;
        seconds = us_to_s(time_duration);
        gbps = data_gb / seconds;

        std::cout << "Write completed with status: " << nixlEnumStrings::statusStr(status) << std::endl;
        std::cout << "- Time: " << format_duration(time_duration) << std::endl;
        std::cout << "- Data: " << std::fixed << std::setprecision(2) << data_gb << " GB" << std::endl;
        std::cout << "- Speed: " << gbps << " GB/s" << std::endl;

        print_segment_title(phase_title("Syncing files"));
        std::cout << "Syncing files to ensure data is written to disk" << std::endl;
        // Sync all files to ensure data is written to disk
        for (i = 0; i < num_transfers; ++i) {
            if (fsync(fd[i]) < 0) {
                std::cerr << "Failed to sync file " << i << " - " << strerror(errno) << std::endl;
                return 1;
            }
            printProgress(float(i + 1) / num_transfers);
        }

        print_segment_title(phase_title("Clearing DRAM buffers"));
        std::cout << "Clearing DRAM buffers" << std::endl;
        for (i = 0; i < num_transfers; ++i) {
            clear_buffer(dram_addr[i].get(), transfer_size);
            printProgress(float(i + 1) / num_transfers);
        }

        print_segment_title(phase_title("File to Memory Transfer (Read Test)"));

        status = agent.createXferReq(NIXL_READ, dram_for_posix_xfer, file_for_posix_xfer, agent_name, treq);
        if (status != NIXL_SUCCESS) {
            std::cerr << "Failed to create read transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
            return 1;
        }

        // Execute read transfer and measure performance
        time_start = nixlTime::getUs();
        status = agent.postXferReq(treq);
        if (status < 0) {
            std::cerr << "Failed to post read transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
            agent.releaseXferReq(treq);
            return 1;
        }

        // Wait for transfer to complete
        do {
            status = agent.getXferStatus(treq);
            if (status < 0) {
                std::cerr << "Error during read transfer - status: " << nixlEnumStrings::statusStr(status) << std::endl;
                agent.releaseXferReq(treq);
                return 1;
            }
        } while (status == NIXL_IN_PROG);

        time_end = nixlTime::getUs();
        time_duration = time_end - time_start;
        total_time += time_duration;

        data_gb = (float(transfer_size) * num_transfers) / (gb_size);
        total_data_gb += data_gb;
        seconds = us_to_s(time_duration);
        gbps = data_gb / seconds;

        std::cout << "Read completed with status: " << nixlEnumStrings::statusStr(status) << std::endl;
        std::cout << "- Time: " << format_duration(time_duration) << std::endl;
        std::cout << "- Data: " << std::fixed << std::setprecision(2) << data_gb << " GB" << std::endl;
        std::cout << "- Speed: " << gbps << " GB/s" << std::endl;

        print_segment_title(phase_title("Validating read data"));

        std::unique_ptr<char[]> expected_buffer = std::make_unique<char[]>(transfer_size);
        fill_test_pattern(expected_buffer.get(), transfer_size);

        for (i = 0; i < num_transfers; ++i) {
            int ret = memcmp(dram_addr[i].get(), expected_buffer.get(), transfer_size);
            if (ret != 0) {
                std::cerr << "DRAM buffer " << i << " validation failed with error: " << ret << std::endl;
                return 1;
            }
            printProgress(float(i + 1) / num_transfers);
        }

        print_segment_title("Freeing resources");

        if (treq) {
            agent.releaseXferReq(treq);
        }

        agent.deregisterMem(file_for_posix);
        agent.deregisterMem(dram_for_posix);

        print_segment_title("TEST SUMMARY");
        std::cout << "Total time: " << format_duration(total_time) << std::endl;
        std::cout << "Total data: " << std::fixed << std::setprecision(2) << total_data_gb << " GB" << std::endl;
        std::cout << line_str << std::endl;

        return ret;
    } catch (const std::exception& e) {
        std::cerr << "Exception during test execution: " << e.what() << std::endl;
        return 1;
    }
}
