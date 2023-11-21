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

import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.LogEntry;
import com.google.longrunning.Operation;
import com.google.longrunning.Operation.ResultCase;
import com.google.protobuf.Message;
import io.grpc.Status.Code;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/** Methods for parsing GRPC log files */
public class LogParserUtils {

  static final String DELIMETER = "---------------------------------------------------------\n";

  // Returns an ExecuteResponse contained in the operation and null if none.
  // If the operation contains an error, returns null and populates StringBuilder "error" with
  // the error message.
  public static <T extends Message> T getExecuteResponse(
      Operation o, Class<T> t, StringBuilder error) throws IOException {
    if (o.getResultCase() == ResultCase.ERROR && o.getError().getCode() != Code.OK.value()) {
      error.append(o.getError());
      return null;
    }
    if (o.getResultCase() == ResultCase.RESPONSE && o.getDone()) {
      return o.getResponse().unpack(t);
    }
    return null;
  }

  /**
   * Attempts to find and print the ExecuteResponse from the details of a log entry. If no
   * Operation could be found in the Watch call responses, or an Operation was found but failed,
   * a failure message is printed.
   */
  private static void printExecuteResponse(List<Operation> responsesList, PrintWriter out)
      throws IOException {
    for (Operation op : responsesList) {
      StringBuilder error = new StringBuilder();
      build.bazel.remote.execution.v2.ExecuteResponse result =
              getExecuteResponse(op, build.bazel.remote.execution.v2.ExecuteResponse.class, error);
      if (result != null) {
        out.println("ExecuteResponse extracted:");
        out.println(result);
      }
      String errString = error.toString();
      if (!errString.isEmpty()) {
        out.printf("ExecuteResponse contained error: %s\n", op.getError());
      }
    }
  }

  /** Prints an individual log entry. */
  static void printLogEntry(LogEntry entry, PrintWriter out) throws IOException {
    out.println(entry.toString());

    switch (entry.getDetails().getDetailsCase()) {
      case EXECUTE:
        out.println(
            "\nAttempted to extract ExecuteResponse from streaming Execute call responses:");
        printExecuteResponse(entry.getDetails().getExecute().getResponsesList(), out);
        break;
      case WAIT_EXECUTION:
        out.println(
            "\nAttempted to extract ExecuteResponse from streaming WaitExecution call responses:");
        printExecuteResponse(entry.getDetails().getWaitExecution().getResponsesList(), out);
        break;
    }
  }
}
