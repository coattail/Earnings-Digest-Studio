import AppKit
import Foundation
import PDFKit
import Vision

struct OCRLine {
    var y: CGFloat
    var parts: [(x: CGFloat, text: String)]
}

func renderedCGImage(for page: PDFPage, scale: CGFloat = 2.0) -> CGImage? {
    let bounds = page.bounds(for: .mediaBox)
    let size = NSSize(width: max(bounds.width * scale, 1), height: max(bounds.height * scale, 1))
    let image = page.thumbnail(of: size, for: .mediaBox)
    var proposedRect = CGRect(origin: .zero, size: image.size)
    return image.cgImage(forProposedRect: &proposedRect, context: nil, hints: nil)
}

func recognizedLines(for page: PDFPage) throws -> [String] {
    guard let cgImage = renderedCGImage(for: page) else {
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

let arguments = Array(CommandLine.arguments.dropFirst())
guard let pdfPath = arguments.first else {
    exit(0)
}

let pageLimit = arguments.count > 1 ? Int(arguments[1]) ?? 18 : 18
guard let document = PDFDocument(url: URL(fileURLWithPath: pdfPath)) else {
    exit(0)
}

let totalPages = min(document.pageCount, max(pageLimit, 1))
for index in 0..<totalPages {
    guard let page = document.page(at: index) else {
        continue
    }
    print("=== page-\(String(format: "%03d", index + 1)) ===")
    do {
        for line in try recognizedLines(for: page) {
            print(line)
        }
    } catch {
        continue
    }
    print("")
}
