package com.kingdee.taxc.controller;

import com.kingdee.taxc.dto.MatchRequest;
import com.kingdee.taxc.dto.MatchResult;
import com.kingdee.taxc.dto.BatchMatchRequest;
import com.kingdee.taxc.service.RedFlushService;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/redflush")
@Validated
public class RedFlushController {
    private final RedFlushService service;

    public RedFlushController(RedFlushService service) {
        this.service = service;
    }

    @PostMapping("/match")
    public ResponseEntity<MatchResult> match(@RequestBody MatchRequest req) {
        return ResponseEntity.ok(service.match(req));
    }

    @PostMapping("/match/batch")
    public ResponseEntity<java.util.List<MatchResult>> batch(@RequestBody BatchMatchRequest req) {
        return ResponseEntity.ok(service.batchMatch(req));
    }

    @PostMapping("/match/temp")
    public ResponseEntity<Void> matchTemp(@RequestBody BatchMatchRequest req) {
        service.batchMatchTempStrategy(req);
        return ResponseEntity.ok().build();
    }
}
