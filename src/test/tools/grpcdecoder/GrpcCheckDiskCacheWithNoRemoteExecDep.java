// Copyright 2023 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package src.test.tools.grpcdecoder;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.OutputFile;
import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog;
import com.google.longrunning.Operation;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Checks that the GRPC logs passed in argument contain the expected calls and reponses, in order:
 * - First, a FindMissingBlobs request that contains file_hash
 * - Then, at least one Write to the CAS for file_hash
 * - Then, an Execute request containing file_hash in the response's output file (the action is
 *   a simple copy, so the hash of the output file is the same as the one used as input).
 */
public class GrpcCheckDiskCacheWithNoRemoteExecDep {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Expected two arguments: grpc_log_file file_hash file_size");
            System.exit(2);
        }
        String grpcLogFile = args[0];
        String fileHash = args[1];
        String fileSizeBytes = args[2];

        checkEntries(grpcLogFile, fileHash, Integer.parseInt(fileSizeBytes));
    }

    private static void print(String msg) {
        System.out.println(msg);
        System.out.print(LogParserUtils.DELIMETER);
    }

    private static void fail(String msg) {
        System.out.println("ERROR: " + msg);
        System.exit(1);
    }

    enum ExpectedStages {
        FIND_MISSING_BLOBS,
        WRITE,
        EXECUTE
    }


    /* We only really care about the presence and order of these methods */
    private static boolean isSkippableMethod(String methodName) {
        return ! (methodName.endsWith("/FindMissingBlobs") ||
                methodName.endsWith("/Write") ||
                methodName.endsWith("/Execute"));
    }

    private static Optional<build.bazel.remote.execution.v2.ExecuteResponse> maybeGetExecuteResponse(
        Operation o) throws IOException {
      StringBuilder error = new StringBuilder();
      build.bazel.remote.execution.v2.ExecuteResponse result = LogParserUtils
              .getExecuteResponse(o, build.bazel.remote.execution.v2.ExecuteResponse.class, error);
      if (result != null) {
        System.out.println("ExecuteResponse extracted:");
        System.out.println(result);
        return Optional.of(result);
      }
      String errString = error.toString();
      if (!errString.isEmpty()) {
        System.out.printf("Operation contained error: %s\n", o.getError());
        return Optional.empty();
      }
      return Optional.empty();
    }

    private static void checkEntries(String grpcLogFile, String fileHash, int fileSizeBytes) throws IOException {
        try (InputStream in = new FileInputStream(grpcLogFile)) {
            PrintWriter out =
                    new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8)), true);
            final Digest fileDigest = Digest.newBuilder().setHash(fileHash).setSizeBytes(fileSizeBytes).build();

            ExpectedStages stage = ExpectedStages.FIND_MISSING_BLOBS;
            RemoteExecutionLog.LogEntry entry;
            while ((entry = RemoteExecutionLog.LogEntry.parseDelimitedFrom(in)) != null) {
                LogParserUtils.printLogEntry(entry, out);
                System.out.print(LogParserUtils.DELIMETER);
                final String methodName = entry.getMethodName();
                if (isSkippableMethod(methodName)) {
                    continue;
                }
                if (stage == ExpectedStages.FIND_MISSING_BLOBS) {
                    if (!methodName.endsWith("/FindMissingBlobs")) {
                        fail("Unexpected method: " + methodName + ". Expected FindMissingBlobs");
                    }
                    if (!entry.getDetails().getFindMissingBlobs().getRequest().getBlobDigestsList().contains(fileDigest)) {
                        fail("Found FindMissingBlob, but missing expected digest: " + fileDigest.getHash());
                    }
                    print("Found first FindMissingBlobs, with expected digest (" + fileHash + ")");
                    stage = ExpectedStages.WRITE;
                } else if (stage == ExpectedStages.WRITE) {
                    if (!methodName.endsWith("/Write")) {
                        fail("Unexpected method: " + methodName + ". Expected at lease one Write for digest: " + fileDigest.getHash());
                    }
                    if (entry.getDetails().getWrite().getResourceNamesList().stream().noneMatch(s -> s.endsWith(fileHash + "/" + fileSizeBytes))) {
                        print("Found Write before first Execute, but not the expected digest yet");
                        continue;
                    }
                    print("Found Write with expected digest (" + fileHash + ")");
                    stage = ExpectedStages.EXECUTE;
                } else /* if (stage == ExpectedStages.EXECUTE) */ {
                    if (methodName.endsWith("/Write")) {
                        // We already have a Write with the right digest. It's ok to have more Write requests.
                        continue;
                    }
                    if (!methodName.endsWith("/Execute")) {
                        fail("Unexpected method: " + methodName + ". Expected Execute");
                    }
                    final Optional<Operation> responseOp = entry.getDetails().getExecute().getResponsesList().stream()
                            .filter(Operation::getDone).findFirst();
                    if (responseOp.isEmpty()) {
                        fail("Found first Execute, but missing response with 'done == True'");
                    }
                    final Optional<ExecuteResponse> executeResponse = maybeGetExecuteResponse(responseOp.get());
                    if (executeResponse.isEmpty()) {
                        fail("Found first Execute, but got unexpected response");
                    }
                    final List<OutputFile> outputFilesList = executeResponse.get().getResult().getOutputFilesList();
                    if (outputFilesList.stream().noneMatch(outputFile -> outputFile.getDigest().equals(fileDigest))) {
                        fail("Found first Execute, but it is missing the expected digest (" + fileHash + ") in output files");
                    }
                    print("Found fist Execute, with expected digest (" + fileHash + ") in its output files");
                    break;
                }
            }
        }
    }
}
