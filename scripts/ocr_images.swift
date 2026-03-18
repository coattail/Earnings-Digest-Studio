import AppKit
import Foundation
import Vision

struct OCRLine {
    var y: CGFloat
    var parts: [(x: CGFloat, text: String)]
}

func recognizedLines(for imagePath: String) throws -> [String] {
    guard let image = NSImage(contentsOfFile: imagePath) else {
        return []
    }
    var proposedRect = CGRect(origin: .zero, size: image.size)
    guard let cgImage = image.cgImage(forProposedRect: &proposedRect, context: nil, hints: nil) else {
        return []
    }

    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = false
    request.recognitionLanguages = ["en-US"]

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])

    let observations = (request.results ?? []).sorted { lhs, rhs in
        let lhsY = lhs.boundingBox.midY
        let rhsY = rhs.boundingBox.midY
        if abs(lhsY - rhsY) > 0.02 {
            return lhsY > rhsY
        }
        return lhs.boundingBox.minX < rhs.boundingBox.minX
    }

    var grouped: [OCRLine] = []
    for observation in observations {
        guard let candidate = observation.topCandidates(1).first else {
            continue
        }
        let text = candidate.string.trimmingCharacters(in: .whitespacesAndNewlines)
        if text.isEmpty {
            continue
        }

        let y = observation.boundingBox.midY
        let x = observation.boundingBox.minX
        if let index = grouped.lastIndex(where: { abs($0.y - y) <= 0.025 }) {
            grouped[index].parts.append((x: x, text: text))
        } else {
            grouped.append(OCRLine(y: y, parts: [(x: x, text: text)]))
        }
    }

    return grouped.map { line in
        line.parts
            .sorted { $0.x < $1.x }
            .map(\.text)
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }.filter { !$0.isEmpty }
}

let imagePaths = Array(CommandLine.arguments.dropFirst())
if imagePaths.isEmpty {
    exit(0)
}

for imagePath in imagePaths {
    let slideName = URL(fileURLWithPath: imagePath).deletingPathExtension().lastPathComponent
    print("=== \(slideName) ===")
    do {
        for line in try recognizedLines(for: imagePath) {
            print(line)
        }
    } catch {
        continue
    }
    print("")
}
