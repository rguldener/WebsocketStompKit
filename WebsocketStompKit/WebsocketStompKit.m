//
//  WebsocketStompKit.m
//  WebsocketStompKit
//
//  Created by Jeff Mesnil on 09/10/2013.
//  Modified by Robin Guldener on 17/03/2015
//  Copyright (c) 2013 Jeff Mesnil & Robin Guldener. All rights reserved.
//

#import "WebsocketStompKit.h"
#import "JFRWebSocket.h"

#define kDefaultTimeout 5
#define kVersion1_2 @"1.2"
#define kNoHeartBeat @"0,0"

#define WSProtocols @[]//@[@"v10.stomp", @"v11.stomp"]

#pragma mark Logging macros

#ifdef DEBUG // set to 1 to enable logs

#define LogDebug(frmt, ...) NSLog(frmt, ##__VA_ARGS__);

#else

#define LogDebug(frmt, ...) {}

#endif

#pragma mark Frame commands

#define kCommandAbort       @"ABORT"
#define kCommandAck         @"ACK"
#define kCommandBegin       @"BEGIN"
#define kCommandCommit      @"COMMIT"
#define kCommandConnect     @"CONNECT"
#define kCommandConnected   @"CONNECTED"
#define kCommandDisconnect  @"DISCONNECT"
#define kCommandError       @"ERROR"
#define kCommandMessage     @"MESSAGE"
#define kCommandNack        @"NACK"
#define kCommandReceipt     @"RECEIPT"
#define kCommandSend        @"SEND"
#define kCommandSubscribe   @"SUBSCRIBE"
#define kCommandUnsubscribe @"UNSUBSCRIBE"

#pragma mark Control characters

#define	kLineFeed @"\x0A"
#define	kNullChar @"\x00"
#define kHeaderSeparator @":"

#pragma mark -
#pragma mark STOMP Client private interface

@interface STOMPClient()

@property (nonatomic, retain) JFRWebSocket *socket;
@property (nonatomic, copy) NSURL *url;
@property (nonatomic, copy) NSString *host;
@property (nonatomic) NSString *clientHeartBeat;
@property (nonatomic, weak) NSTimer *pinger;
@property (nonatomic, weak) NSTimer *ponger;
@property (nonatomic, assign) BOOL heartbeat;

@property (nonatomic, copy) void (^disconnectedHandler)(NSError *error);
@property (nonatomic, copy) void (^connectionCompletionHandler)(STOMPFrame *connectedFrame, NSError *error);
@property (nonatomic, copy) NSDictionary *connectFrameHeaders;
@property (nonatomic, retain) NSMutableDictionary *subscriptions;

- (void) sendFrameWithCommand:(NSString *)command
                      headers:(NSDictionary *)headers
                         body:(NSString *)body;

@end

#pragma mark STOMP Frame

@interface STOMPFrame()

- (id)initWithCommand:(NSString *)theCommand
              headers:(NSDictionary *)theHeaders
                 body:(NSString *)theBody;

- (NSData *)toData;

@end

@implementation STOMPFrame

@synthesize command, headers, body;

- (id)initWithCommand:(NSString *)theCommand
              headers:(NSDictionary *)theHeaders
                 body:(NSString *)theBody {
    if(self = [super init]) {
        command = theCommand;
        headers = theHeaders;
        body = theBody;
    }
    return self;
}

- (NSString *)toString {
    NSMutableString *frame = [NSMutableString stringWithString: [self.command stringByAppendingString:kLineFeed]];
    for (id key in self.headers) {
        [frame appendString:[NSString stringWithFormat:@"%@%@%@%@", key, kHeaderSeparator, self.headers[key], kLineFeed]];
    }
    [frame appendString:kLineFeed];
    if (self.body) {
        [frame appendString:self.body];
    }
    [frame appendString:kNullChar];
    return frame;
}

- (NSData *)toData {
    return [[self toString] dataUsingEncoding:NSUTF8StringEncoding];
}

+ (STOMPFrame *) STOMPFrameFromData:(NSData *)data {
    NSData *strData = [data subdataWithRange:NSMakeRange(0, [data length])];
    NSString *msg = [[NSString alloc] initWithData:strData encoding:NSUTF8StringEncoding];
    LogDebug(@"<<< %@", msg);
    NSMutableArray *contents = (NSMutableArray *)[[msg componentsSeparatedByString:kLineFeed] mutableCopy];
    while ([contents count] > 0 && [contents[0] isEqual:@""]) {
        [contents removeObjectAtIndex:0];
    }
    NSString *command = [[contents objectAtIndex:0] copy];
    NSMutableDictionary *headers = [[NSMutableDictionary alloc] init];
    NSMutableString *body = [[NSMutableString alloc] init];
    BOOL hasHeaders = NO;
    [contents removeObjectAtIndex:0];
    for(NSString *line in contents) {
        if(hasHeaders) {
            for (int i=0; i < [line length]; i++) {
                unichar c = [line characterAtIndex:i];
                if (c != '\x00') {
                    [body appendString:[NSString stringWithFormat:@"%c", c]];
                }
            }
        } else {
            if ([line isEqual:@""]) {
                hasHeaders = YES;
            } else {
                NSMutableArray *parts = [NSMutableArray arrayWithArray:[line componentsSeparatedByString:kHeaderSeparator]];
                // key ist the first part
                NSString *key = parts[0];
                [parts removeObjectAtIndex:0];
                headers[key] = [parts componentsJoinedByString:kHeaderSeparator];
            }
        }
    }
    return [[STOMPFrame alloc] initWithCommand:command headers:headers body:body];
}

- (NSString *)description {
    return [self toString];
}


@end

#pragma mark STOMP Message

@interface STOMPMessage()

@property (nonatomic, retain) STOMPClient *client;

+ (STOMPMessage *)STOMPMessageFromFrame:(STOMPFrame *)frame
                                 client:(STOMPClient *)client;

@end

@implementation STOMPMessage

@synthesize client;

- (id)initWithClient:(STOMPClient *)theClient
             headers:(NSDictionary *)theHeaders
                body:(NSString *)theBody {
    if (self = [super initWithCommand:kCommandMessage
                              headers:theHeaders
                                 body:theBody]) {
        self.client = theClient;
    }
    return self;
}

- (void)ack {
    [self ackWithCommand:kCommandAck headers:nil];
}

- (void)ack: (NSDictionary *)theHeaders {
    [self ackWithCommand:kCommandAck headers:theHeaders];
}

- (void)nack {
    [self ackWithCommand:kCommandNack headers:nil];
}

- (void)nack: (NSDictionary *)theHeaders {
    [self ackWithCommand:kCommandNack headers:theHeaders];
}

- (void)ackWithCommand: (NSString *)command
               headers: (NSDictionary *)theHeaders {
    NSMutableDictionary *ackHeaders = [[NSMutableDictionary alloc] initWithDictionary:theHeaders];
    ackHeaders[kHeaderID] = self.headers[kHeaderAck];
    [self.client sendFrameWithCommand:command
                              headers:ackHeaders
                                 body:nil];
}

+ (STOMPMessage *)STOMPMessageFromFrame:(STOMPFrame *)frame
                                 client:(STOMPClient *)client {
    return [[STOMPMessage alloc] initWithClient:client headers:frame.headers body:frame.body];
}

@end

#pragma mark STOMP Subscription

@interface STOMPSubscription()

@property (nonatomic, retain) STOMPClient *client;

- (id)initWithClient:(STOMPClient *)theClient
          identifier:(NSString *)theIdentifier;

@end

@implementation STOMPSubscription

@synthesize client;
@synthesize identifier;

- (id)initWithClient:(STOMPClient *)theClient
          identifier:(NSString *)theIdentifier {
    if(self = [super init]) {
        self.client = theClient;
        identifier = [theIdentifier copy];
    }
    return self;
}

- (void)unsubscribe {
    [self.client sendFrameWithCommand:kCommandUnsubscribe
                              headers:@{kHeaderID: self.identifier}
                                 body:nil];
}

- (NSString *)description {
    return [NSString stringWithFormat:@"<STOMPSubscription identifier:%@>", identifier];
}

@end

#pragma mark STOMP Transaction

@interface STOMPTransaction()

@property (nonatomic, retain) STOMPClient *client;

- (id)initWithClient:(STOMPClient *)theClient
          identifier:(NSString *)theIdentifier;

@end

@implementation STOMPTransaction

@synthesize identifier;

- (id)initWithClient:(STOMPClient *)theClient
          identifier:(NSString *)theIdentifier {
    if(self = [super init]) {
        self.client = theClient;
        identifier = [theIdentifier copy];
    }
    return self;
}

- (void)commit {
    [self.client sendFrameWithCommand:kCommandCommit
                              headers:@{kHeaderTransaction: self.identifier}
                                 body:nil];
}

- (void)abort {
    [self.client sendFrameWithCommand:kCommandAbort
                              headers:@{kHeaderTransaction: self.identifier}
                                 body:nil];
}

- (NSString *)description {
    return [NSString stringWithFormat:@"<STOMPTransaction identifier:%@>", identifier];
}

@end

#pragma mark STOMP Client Implementation

@implementation STOMPClient

@synthesize socket, url, host, heartbeat;
@synthesize connectFrameHeaders;
@synthesize connectionCompletionHandler, disconnectedHandler, receiptHandler, errorHandler;
@synthesize subscriptions;
@synthesize pinger, ponger;
@synthesize delegate;

int idGenerator;
CFAbsoluteTime serverActivity;

#pragma mark -
#pragma mark Public API

- (id)initWithURL:(NSURL *)theUrl webSocketHeaders:(NSDictionary *)headers useHeartbeat:(BOOL)heart {
    if(self = [super init]) {
        self.socket = [[JFRWebSocket alloc] initWithURL:theUrl protocols:WSProtocols];
        if (headers) {
            for (NSString *key in headers.allKeys) {
                [self.socket addHeader:[headers objectForKey:key] forKey:key];
            }
        }
        self.socket.delegate = self;
        
        self.heartbeat = heart;
        
        self.url = theUrl;
        self.host = theUrl.host;
        idGenerator = 0;
        self.connected = NO;
        self.subscriptions = [[NSMutableDictionary alloc] init];
        self.clientHeartBeat = @"5000,10000";
    }
    return self;
}

- (BOOL) heartbeatActivated {
    return heartbeat;
}

- (void)connectWithLogin:(NSString *)login
                passcode:(NSString *)passcode
       completionHandler:(void (^)(STOMPFrame *connectedFrame, NSError *error))completionHandler {
    [self connectWithHeaders:@{kHeaderLogin: login, kHeaderPasscode: passcode}
           completionHandler:completionHandler];
}

- (void)connectWithHeaders:(NSDictionary *)headers
         completionHandler:(void (^)(STOMPFrame *connectedFrame, NSError *error))completionHandler {
    self.connectFrameHeaders = headers;
    self.connectionCompletionHandler = completionHandler;
    [self.socket connect];
}

- (void)sendTo:(NSString *)destination
          body:(NSString *)body {
    [self sendTo:destination
         headers:nil
            body:body];
}

- (void)sendTo:(NSString *)destination
       headers:(NSDictionary *)headers
          body:(NSString *)body {
    NSMutableDictionary *msgHeaders = [NSMutableDictionary dictionaryWithDictionary:headers];
    msgHeaders[kHeaderDestination] = destination;
    if (body) {
        msgHeaders[kHeaderContentLength] = [NSNumber numberWithLong:[body length]];
    }
    [self sendFrameWithCommand:kCommandSend
                       headers:msgHeaders
                          body:body];
}

- (STOMPSubscription *)subscribeTo:(NSString *)destination
                    messageHandler:(STOMPMessageHandler)handler {
    return [self subscribeTo:destination
                     headers:nil
              messageHandler:handler];
}

- (STOMPSubscription *)subscribeTo:(NSString *)destination
                           headers:(NSDictionary *)headers
                    messageHandler:(STOMPMessageHandler)handler {
    NSMutableDictionary *subHeaders = [[NSMutableDictionary alloc] initWithDictionary:headers];
    subHeaders[kHeaderDestination] = destination;
    NSString *identifier = subHeaders[kHeaderID];
    if (!identifier) {
        identifier = [NSString stringWithFormat:@"sub-%d", idGenerator++];
        subHeaders[kHeaderID] = identifier;
    }
    self.subscriptions[identifier] = handler;
    [self sendFrameWithCommand:kCommandSubscribe
                       headers:subHeaders
                          body:nil];
    return [[STOMPSubscription alloc] initWithClient:self identifier:identifier];
}

- (STOMPTransaction *)begin {
    NSString *identifier = [NSString stringWithFormat:@"tx-%d", idGenerator++];
    return [self begin:identifier];
}

- (STOMPTransaction *)begin:(NSString *)identifier {
    [self sendFrameWithCommand:kCommandBegin
                       headers:@{kHeaderTransaction: identifier}
                          body:nil];
    return [[STOMPTransaction alloc] initWithClient:self identifier:identifier];
}

- (void)disconnect {
    [self disconnect: nil];
}

- (void)disconnect:(void (^)(NSError *error))completionHandler {
    self.disconnectedHandler = completionHandler;
    [self sendFrameWithCommand:kCommandDisconnect
                       headers:nil
                          body:nil];
    [self.subscriptions removeAllObjects];
    [self.pinger invalidate];
    [self.ponger invalidate];
    [self.socket disconnect];
}


#pragma mark -
#pragma mark Private Methods

- (void)sendFrameWithCommand:(NSString *)command
                     headers:(NSDictionary *)headers
                        body:(NSString *)body {
    if (![self.socket isConnected]) {
        return;
    }
    STOMPFrame *frame = [[STOMPFrame alloc] initWithCommand:command headers:headers body:body];
    LogDebug(@">>> %@", frame);
    [self.socket writeString:[frame toString]];
}

- (void)sendPing:(NSTimer *)timer  {
    if (![self.socket isConnected]) {
        return;
    }
    [self.socket writeData:[NSData dataWithBytes:"\x0A" length:1]];
    LogDebug(@">>> PING");
}

- (void)checkPong:(NSTimer *)timer  {
    NSDictionary *dict = timer.userInfo;
    NSInteger ttl = [dict[@"ttl"] intValue];
    
    CFAbsoluteTime delta = CFAbsoluteTimeGetCurrent() - serverActivity;
    if (delta > (ttl * 2)) {
        LogDebug(@"did not receive server activity for the last %f seconds", delta);
        [self disconnect:errorHandler];
    }
}

- (void)setupHeartBeatWithClient:(NSString *)clientValues
                          server:(NSString *)serverValues {
    if (!heartbeat) {
        return;
    }
    
    NSInteger cx, cy, sx, sy;
    
    NSScanner *scanner = [NSScanner scannerWithString:clientValues];
    scanner.charactersToBeSkipped = [NSCharacterSet characterSetWithCharactersInString:@", "];
    [scanner scanInteger:&cx];
    [scanner scanInteger:&cy];
    
    scanner = [NSScanner scannerWithString:serverValues];
    scanner.charactersToBeSkipped = [NSCharacterSet characterSetWithCharactersInString:@", "];
    [scanner scanInteger:&sx];
    [scanner scanInteger:&sy];
    
    NSInteger pingTTL = ceil(MAX(cx, sy) / 1000);
    NSInteger pongTTL = ceil(MAX(sx, cy) / 1000);
    
    LogDebug(@"send heart-beat every %ld seconds", pingTTL);
    LogDebug(@"expect to receive heart-beats every %ld seconds", pongTTL);
    
    dispatch_async(dispatch_get_main_queue(), ^{
        if (pingTTL > 0) {
            self.pinger = [NSTimer scheduledTimerWithTimeInterval: pingTTL
                                                           target: self
                                                         selector: @selector(sendPing:)
                                                         userInfo: nil
                                                          repeats: YES];
        }
        if (pongTTL > 0) {
            self.ponger = [NSTimer scheduledTimerWithTimeInterval: pongTTL
                                                           target: self
                                                         selector: @selector(checkPong:)
                                                         userInfo: @{@"ttl": [NSNumber numberWithInteger:pongTTL]}
                                                          repeats: YES];
        }
    });
    
}

- (void)receivedFrame:(STOMPFrame *)frame {
    // CONNECTED
    if([kCommandConnected isEqual:frame.command]) {
        self.connected = YES;
        [self setupHeartBeatWithClient:self.clientHeartBeat server:frame.headers[kHeaderHeartBeat]];
        if (self.connectionCompletionHandler) {
            self.connectionCompletionHandler(frame, nil);
        }
        // MESSAGE
    } else if([kCommandMessage isEqual:frame.command]) {
        STOMPMessageHandler handler = self.subscriptions[frame.headers[kHeaderSubscription]];
        if (handler) {
            STOMPMessage *message = [STOMPMessage STOMPMessageFromFrame:frame
                                                                 client:self];
            handler(message);
        } else {
            //TODO default handler
        }
        // RECEIPT
    } else if([kCommandReceipt isEqual:frame.command]) {
        if (self.receiptHandler) {
            self.receiptHandler(frame);
        }
        // ERROR
    } else if([kCommandError isEqual:frame.command]) {
        NSError *error = [[NSError alloc] initWithDomain:@"StompKit" code:1 userInfo:@{@"frame": frame}];
        // ERROR coming after the CONNECT frame
        if (!self.connected && self.connectionCompletionHandler) {
            self.connectionCompletionHandler(frame, error);
        } else if (self.errorHandler) {
            self.errorHandler(error);
        } else {
            LogDebug(@"Unhandled ERROR frame: %@", frame);
        }
    } else {
        NSError *error = [[NSError alloc] initWithDomain:@"StompKit"
                                                    code:2
                                                userInfo:@{@"message": [NSString stringWithFormat:@"Unknown frame %@", frame.command],
                                                           @"frame": frame}];
        if (self.errorHandler) {
            self.errorHandler(error);
        }
    }
}

#pragma mark -
#pragma mark JetfireDelegate

- (void) websocketDidConnect:(JFRWebSocket*) socket {
    
    // Websocket has connected, send the STOMP connection frame
    NSMutableDictionary *connectHeaders = [[NSMutableDictionary alloc] initWithDictionary:connectFrameHeaders];
    connectHeaders[kHeaderAcceptVersion] = kVersion1_2;
    if (!connectHeaders[kHeaderHost]) {
        connectHeaders[kHeaderHost] = host;
    }
    if (!connectHeaders[kHeaderHeartBeat]) {
        connectHeaders[kHeaderHeartBeat] = self.clientHeartBeat;
    } else {
        self.clientHeartBeat = connectHeaders[kHeaderHeartBeat];
    }
    
    [self sendFrameWithCommand:kCommandConnect
                       headers:connectHeaders
                          body: nil];
    
}

// BINARY FRAMES!
// Should never be used for STOMP as STOMP is a text based protocol.
// However, STOMPKit can handle binary data so no harm in leaving this here
- (void)websocket:(JFRWebSocket*)socket didReceiveData:(NSData*)data {
    serverActivity = CFAbsoluteTimeGetCurrent();
    STOMPFrame *frame = [STOMPFrame STOMPFrameFromData:data];
    [self receivedFrame:frame];
}

// TEXT FRAMES!
// This is where all the goodness should arrive
- (void)websocket:(JFRWebSocket*)socket didReceiveMessage:(NSString *)string {
    serverActivity = CFAbsoluteTimeGetCurrent();
    STOMPFrame *frame = [STOMPFrame STOMPFrameFromData:[string dataUsingEncoding:NSUTF8StringEncoding]];
    [self receivedFrame:frame];
}

- (void)websocketDidDisconnect:(JFRWebSocket*)socket error:(NSError*)error {
    LogDebug(@"socket did disconnect, error: %@", error);
    if (!self.connected && self.connectionCompletionHandler) {
        self.connectionCompletionHandler(nil, error);
    } else if (self.connected) {
        if (self.disconnectedHandler) {
            self.disconnectedHandler(error);
        } else if (self.errorHandler) {
            self.errorHandler(error);
        }
    }
    self.connected = NO;
    
    if (self.delegate != nil && [self.delegate respondsToSelector:@selector(websocketDidDisconnect:)]) {
        [self.delegate websocketDidDisconnect:error];
    }
}

@end
